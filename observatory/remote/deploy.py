# Deploy history of the Subcon checkouts: HEAD + reflog (sha, time, action verb). No commit messages.
# Config: DEPLOY_GIT_GLOBS (comma-separated glob patterns of git checkouts)
import glob, subprocess

dirs = sorted({d for pat in CONFIG["DEPLOY_GIT_GLOBS"].split(",") if pat for d in glob.glob(pat) if os.path.isdir(os.path.join(d, ".git"))})
heads = Table("heads", [("checkout_idx", "int"), ("sha", "enum"), ("committed_at", "ts"), ("dir_mtime", "ts")])
reflog = Table("reflog", [("checkout_idx", "int"), ("sha", "enum"), ("at", "ts"), ("action", "enum")])


def git(d, *args):
    return subprocess.run(["git", "-C", d, *args], capture_output=True, text=True).stdout


for i, d in enumerate(dirs):
    sha, _, at = git(d, "log", "-1", "--format=%h %cI").strip().partition(" ")
    heads.add(i, sha, at, os.path.getmtime(d))
    for line in git(d, "reflog", "--date=iso-strict", "--format=%h|%gd|%gs", "-n", "200").splitlines():
        sha, selector, subject = (line.split("|", 2) + ["", ""])[:3]
        at = selector[selector.find("{") + 1:selector.rfind("}")] if "{" in selector else None
        action = subject.split(":", 1)[0].split(" ", 1)[0].lower() if subject else None
        reflog.add(i, sha, at, action)
emit("deploy", [heads, reflog], {"n_checkouts": len(dirs)})
