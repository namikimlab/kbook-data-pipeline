import os, sys, time, subprocess, shlex

PROJECT_DIR = "/home/ec2-user/kbook-data-pipeline"
STATE_DIR   = "/home/ec2-user/nlk-state"
MONTHS_FILE = os.path.join(STATE_DIR, "months.list")
FETCH_SCRIPT= os.path.join(PROJECT_DIR, "scripts", "fetch_pages_month.py")
PYTHON_BIN  = os.path.join(PROJECT_DIR, "venv", "bin", "python")

PAGE_SIZE   = os.environ.get("NLK_PAGE_SIZE", "100")     
SLEEP_BETWEEN_MONTHS_SEC = 3                            # polite pacing

def run_month(ym: str) -> int:
    """Run one month by calling the existing month script. Return its exit code."""
    y, m = ym.split("-")
    cmd = f'{shlex.quote(PYTHON_BIN)} {shlex.quote(FETCH_SCRIPT)} --year {int(y)} --month {int(m)} --page-size {int(PAGE_SIZE)} --state-dir {shlex.quote(STATE_DIR)}'
    print(f"▶ Running month {ym}: {cmd}", flush=True)
    proc = subprocess.run(cmd, shell=True)
    return proc.returncode

def month_done_path(ym: str) -> str:
    return os.path.join(STATE_DIR, f"{ym}.done")

def month_page_path(ym: str) -> str:
    return os.path.join(STATE_DIR, f"{ym}.page")

def main():
    if not os.path.exists(MONTHS_FILE):
        print(f"ERROR: months.list not found at {MONTHS_FILE}", file=sys.stderr)
        sys.exit(2)

    with open(MONTHS_FILE, "r") as f:
        months = [ln.strip() for ln in f if ln.strip()]

    for ym in months:
        done_file = month_done_path(ym)
        if os.path.exists(done_file):
            print(f"✓ {ym} already done. Skipping.")
            continue

        # Run this month until it succeeds or a hard error occurs.
        rc = run_month(ym)
        if rc == 0:
            # Month finished. Create .done and clean up .page (if any).
            try:
                open(done_file, "w").close()
                page_file = month_page_path(ym)
                if os.path.exists(page_file):
                    os.remove(page_file)
                print(f"✅ {ym} marked done.")
            except Exception as e:
                print(f"WARNING: Could not finalize {ym}: {e}", file=sys.stderr)
            time.sleep(SLEEP_BETWEEN_MONTHS_SEC)
            continue
        else:
            # Non-zero means temporary failure (e.g., network). Exit and let systemd restart us.
            print(f"⚠️  {ym} failed with code {rc}. Exiting so systemd can restart & resume.", file=sys.stderr)
            sys.exit(rc)

    print("🎉 All months complete. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    main()