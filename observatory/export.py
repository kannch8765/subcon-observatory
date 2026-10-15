"""Run the remote exporters and unpack their output into a new snapshot directory.

    uv run python -m observatory.export                 # all exporters, via SSH_HOST
    uv run python -m observatory.export --only store    # a subset
    uv run python -m observatory.export --local         # run on this machine (tests / dry runs)
"""
import argparse, datetime, io, json, pathlib, subprocess, sys, tarfile

REMOTE = pathlib.Path(__file__).parent / "remote"
EXPORTERS = {
    "store": ["STORE_DIR"],
    "whispers": ["WHISPER_DIRS"],
    "backlog": ["CLAUDE_HOME"],
    "recall_log": ["RECALL_LOG_GLOBS"],
    "deploy": ["DEPLOY_GIT_GLOBS"],
}


def load_config(path):
    config = {}
    for line in pathlib.Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            config[key.strip()] = value.strip().strip('"').strip("'")
    return config


def build_script(name, config):
    """CONFIG preamble (only the keys this exporter needs) + sanitizer + exporter, as one stdin script."""
    needed = {k: config[k] for k in EXPORTERS[name]}
    return "\n".join([f"CONFIG = {json.dumps(needed)}", (REMOTE / "_sanitize.py").read_text(), (REMOTE / f"{name}.py").read_text()])


def run_exporter(name, config, local=False):
    cmd = [sys.executable, "-I", "-"] if local else ["ssh", config["SSH_HOST"], "python3", "-I", "-"]
    proc = subprocess.run(cmd, input=build_script(name, config).encode(), capture_output=True)
    sys.stderr.write(proc.stderr.decode(errors="replace"))
    if proc.returncode != 0:
        raise RuntimeError(f"exporter {name} failed with exit code {proc.returncode}")
    return proc.stdout


def git_sha():
    out = subprocess.run(["git", "-C", str(REMOTE), "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    return out.stdout.strip() or None


def export(config, only=None, local=False, snapshot_dir=None):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    snap = pathlib.Path(snapshot_dir or pathlib.Path(config["SNAPSHOT_ROOT"]).expanduser() / stamp)
    snap.mkdir(parents=True, exist_ok=False)
    summary = {"snapshot": snap.name, "observatory_sha": git_sha(), "exporters": {}}
    for name in only or EXPORTERS:
        print(f"[{name}]", file=sys.stderr)
        try:
            data = run_exporter(name, config, local)
        except Exception as error:  # keep the other exporters going; record the failure
            summary["exporters"][name] = {"error": str(error)}
            continue
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(snap, filter="data")
        summary["exporters"][name] = json.loads((snap / name / "manifest.json").read_text())["tables"]
    (snap / "snapshot.json").write_text(json.dumps(summary, indent=2))
    return snap


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config.local.env")
    parser.add_argument("--only", help="comma-separated exporters: " + ",".join(EXPORTERS))
    parser.add_argument("--local", action="store_true", help="run exporters on this machine instead of over SSH")
    args = parser.parse_args()
    only = args.only.split(",") if args.only else None
    if only and (unknown := set(only) - set(EXPORTERS)):
        parser.error(f"unknown exporters: {', '.join(sorted(unknown))}")
    snap = export(load_config(args.config), only, args.local)
    print(snap)


if __name__ == "__main__":
    main()
