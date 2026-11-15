"""Trace archive step 4: the daily retention run. Archive, then delete, the provider_traces rows that have left the
window, then encrypt a snapshot of the archive for its off-site copy.

    python3 -E -s -m observatory.trace_retention --config <file> --archive-dir <dir> \\
        --offsite-dir <dir> --age-recipient <age1...> --status <file>

Both steps get one cutoff and one last day: the day (older-than-days + 1) days ago in TELEMETRY_TZ (the server's zone
when unset). So they see the same rows, no day is split between two nights, and rows stay 30 to 31 days. Rows newer
than the cutoff stay whatever zone Postgres uses for days: the cutoff is in every query too. --delete runs only if
--archive exited 0, and it still re-checks every row itself (docs/TRACE-ARCHIVE.md).

After a delete the archive holds the only copy of those rows. With --offsite-dir the run always ends (whatever happened
before) with a consistent copy of the archive (SQLite's backup API, under the archive's lock) encrypted with age to the
owner's public key, and a manifest of its sizes and sha256s for the pull to check. The server never holds the private
key. The last snapshot is replaced only by a complete one that passes SQLite's quick_check and holds at least as many
verified rows.

Prints counts, sizes and hashes only. An unexpected error prints its class name; its traceback goes to a root-only file
in the archive directory. --status writes the run's exits and counts as JSON (for the health check).

Exit codes: 0 all good; 3 --archive stopped for disk space (nothing deleted); 4 --archive or --delete reported rows that
need a look, or the VACUUM failed or warned (see their output); 5 the snapshot failed (the deletes stand, the last
snapshot is older); 1 an unexpected error.
"""
import argparse, datetime, hashlib, json, os, re, shutil, sqlite3, stat, subprocess, sys, traceback, zoneinfo

from observatory import trace_archive as ta
from observatory.export import load_config

SNAPSHOT = ta.ARCHIVE_DB + ".age"
MANIFEST = SNAPSHOT + ".json"
ERROR_LOG = "retention-error.log"
AGE_HEADER = b"age-encryption.org/v1\n"
_RECIPIENT = re.compile(r"^age1[02-9ac-hj-np-z]{58}$")  # an age X25519 public key (bech32)
AGE = "age"  # tests point this at a stand-in


def last_day(config, older_than_days, now):
    """The newest whole day both steps take, as YYYY-MM-DD."""
    tz = zoneinfo.ZoneInfo(config["TELEMETRY_TZ"]) if config.get("TELEMETRY_TZ") else now.astimezone().tzinfo
    return (now.astimezone(tz) - datetime.timedelta(days=older_than_days + 1)).date().isoformat()


def _last_run(directory):
    """(args, totals) of the archive's newest run; totals is None while that run hasn't finished."""
    db = sqlite3.connect(f"file:{os.path.join(directory, ta.ARCHIVE_DB)}?mode=ro", uri=True)
    try:
        args, totals = db.execute("select args, totals from runs order by run desc limit 1").fetchone()
    finally:
        db.close()
    return args, json.loads(totals) if totals else None


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path, value):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(value, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def snapshot(directory, offsite_dir, recipient, min_free_gb=ta.DEFAULT_MIN_FREE_GB):
    """Encrypt a consistent copy of the archive to `recipient` as offsite_dir/SNAPSHOT and write its manifest.
    Returns the manifest. The plaintext copy exists only while it's encrypted and is removed on every path but a hard
    kill (the next run removes it then). The last snapshot is kept when the copy fails SQLite's quick_check or holds
    fewer verified rows than it (the archive only ever adds rows): a damaged or wrong archive never replaces it."""
    if not os.path.isfile(os.path.join(directory, ta.ARCHIVE_DB)):  # open_archive would create an empty one
        raise RuntimeError("no archive in that directory")
    os.makedirs(offsite_dir, mode=0o700, exist_ok=True)  # as the archive's own directory (open_archive)
    st = os.stat(offsite_dir)
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) & 0o077:
        raise RuntimeError(f"off-site directory must belong to uid {os.getuid()} with mode 700")
    plain = os.path.join(offsite_dir, ".snapshot.sqlite3")
    part = os.path.join(offsite_dir, f".{SNAPSHOT}.part")
    db, path, lock = ta.open_archive(directory)  # its lock: no archive or delete run writes while we copy
    try:
        for leftover in (plain, part):
            if os.path.exists(leftover):
                os.remove(leftover)
        # the plaintext copy and the new snapshot sit next to the last snapshot until it's replaced
        if shutil.disk_usage(offsite_dir).free < 2 * os.path.getsize(path) + min_free_gb * 1e9:
            raise RuntimeError(f"not enough free space for the snapshot (keeps {min_free_gb:g} GB free)")
        ta._private_file(plain)
        copy = sqlite3.connect(plain)
        try:
            db.backup(copy)
            if copy.execute("pragma quick_check").fetchall() != [("ok",)]:
                raise RuntimeError("the archive's copy fails SQLite's quick_check")
            rows, first, last = copy.execute("select count(*), min(day), max(day) from traces "
                                             "where verified_at is not null").fetchone()
        finally:
            copy.close()
        manifest_path = os.path.join(offsite_dir, MANIFEST)
        if os.path.isfile(manifest_path) and rows < json.load(open(manifest_path)).get("verified_rows", 0):
            raise RuntimeError("fewer verified rows than the last snapshot")
        manifest = {"created_at": ta._now(), "recipient": recipient, "plaintext_bytes": os.path.getsize(plain),
                    "plaintext_sha256": _sha256(plain), "verified_rows": rows, "verified_days": [first, last]}
        subprocess.run([AGE, "--encrypt", "--recipient", recipient, "--output", part, plain],
                       capture_output=True, check=True)
        with open(part, "rb") as f:
            if f.read(len(AGE_HEADER)) != AGE_HEADER:
                raise RuntimeError("age wrote no age file")
        os.chmod(part, 0o600)
        manifest.update(bytes=os.path.getsize(part), sha256=_sha256(part))
        os.replace(part, os.path.join(offsite_dir, SNAPSHOT))
        _write_json(manifest_path, manifest)  # a stop right before this leaves the old manifest: the pull's check fails
        return manifest
    finally:
        for leftover in (plain, part):
            if os.path.exists(leftover):
                os.remove(leftover)
        db.close()
        lock.close()


def run(config, directory, older_than_days=ta.DEFAULT_OLDER_THAN_DAYS, offsite_dir=None, recipient=None,
        status_path=None, min_free_gb=ta.DEFAULT_MIN_FREE_GB, dry_run=False, now=None, out=sys.stdout):
    """Archive, then (if that exited 0) delete, through last_day(); then the snapshot and the status. Returns the exit
    code; an unexpected error is raised after the snapshot and the status are written."""
    now = now or datetime.datetime.now(datetime.timezone.utc)  # one cutoff for both steps
    status = {"started_at": ta._now(), "last_day": None, "cutoff": None, "dry_run": dry_run, "archive": None,
              "delete": None, "snapshot": None, "error": None}
    error = None
    try:
        day = status["last_day"] = last_day(config, older_than_days, now)
        status["cutoff"] = ta.cutoff_time(older_than_days, now)
        out.write(f"== trace retention: rows older than {older_than_days} days, through {day}\n== archive\n")
        code = ta.run_archive(config, directory, older_than_days, date_to=day, min_free_gb=min_free_gb, now=now, out=out)
        status["archive"] = {"exit": code, "totals": _last_run(directory)[1]}
        if code == 0:
            out.write("== delete\n")
            code = ta.run_delete(config, directory, older_than_days, date_to=day, dry_run=dry_run, now=now, out=out)
            status["delete"] = {"exit": code, "totals": _last_run(directory)[1]}
        else:
            out.write(f"== delete skipped: --archive exited {code}\n")
    except Exception as exc:  # rows may already be gone: the snapshot and the status still have to happen
        error, code = exc, 1
        status["error"] = type(exc).__name__
        if status["archive"] and not status["delete"]:  # the delete got as far as its runs row: keep its counts
            args, totals = _last_run(directory)
            if args.startswith("delete"):
                status["delete"] = {"exit": None, "totals": totals}
    if offsite_dir:
        try:
            manifest = snapshot(directory, offsite_dir, recipient, min_free_gb)
            status["snapshot"] = manifest
            out.write(f"== snapshot: {manifest['verified_rows']} verified rows ({manifest['verified_days'][0]}.."
                      f"{manifest['verified_days'][1]}), {manifest['bytes']} B, sha256 {manifest['sha256']}\n")
        except Exception as exc:
            status["snapshot"] = {"error": type(exc).__name__}
            out.write(f"== snapshot failed: {type(exc).__name__}; the last snapshot is older\n")
            code = code or 5
    status.update(finished_at=ta._now(), exit=code)
    if status_path:
        try:
            _write_json(status_path, status)
            os.chmod(status_path, 0o640)  # as the collector's files in TELEMETRY_DIR
        except OSError as exc:
            out.write(f"== status not written: {type(exc).__name__}\n")
            error = error or exc  # never hides an earlier error
    if error:
        raise error
    out.write(f"== exit {code}\n")
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", required=True, help="observatory config (Letta psql settings, TELEMETRY_DIR)")
    parser.add_argument("--archive-dir", required=True, help="archive directory: owned by the running user, mode 700")
    parser.add_argument("--offsite-dir", help="where the encrypted snapshot goes: owned by the running user, mode 700")
    parser.add_argument("--age-recipient", help="the owner's age public key (age1...); needed with --offsite-dir")
    parser.add_argument("--status", help="JSON file for the run's exits and counts")
    parser.add_argument("--older-than-days", type=int, default=ta.DEFAULT_OLDER_THAN_DAYS)
    parser.add_argument("--min-free-gb", type=float, default=ta.DEFAULT_MIN_FREE_GB,
                        help="free space --archive and the snapshot leave on their file systems")
    parser.add_argument("--dry-run", action="store_true", help="archive for real, then --delete --dry-run")
    args = parser.parse_args()
    if args.older_than_days < ta.MIN_DELETE_DAYS:
        parser.error(f"--older-than-days must be at least {ta.MIN_DELETE_DAYS}")
    if bool(args.offsite_dir) != bool(args.age_recipient):
        parser.error("--offsite-dir and --age-recipient go together")
    if args.age_recipient and not _RECIPIENT.match(args.age_recipient):
        parser.error("--age-recipient must be an age public key (age1...)")
    os.umask(0o077)
    try:
        code = run(load_config(args.config), args.archive_dir, args.older_than_days, args.offsite_dir,
                   args.age_recipient, args.status, args.min_free_gb, args.dry_run)
    except Exception as exc:
        log = os.path.join(args.archive_dir, ERROR_LOG)
        try:
            with open(log, "a") as f:
                f.write(f"== {ta._now()}\n{traceback.format_exc()}")
            where = f"traceback in {ERROR_LOG} in the archive directory"
        except OSError:
            where = "traceback not written"
        print(f"== failed: {type(exc).__name__} ({where})")
        sys.exit(1)
    sys.exit(code)


if __name__ == "__main__":
    main()
