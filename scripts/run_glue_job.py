"""Ops utility: start a Glue job and block until it finishes (manual reruns outside Airflow).

  python scripts/run_glue_job.py --job marriott-dp-dev-bronze-to-silver --date 2026-11-09
Exit code 0 on SUCCEEDED, 1 otherwise, so the Jenkins stage fails and alerts.
"""
import argparse
import sys
import time

import boto3

TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--job", required=True)
    p.add_argument("--date", required=True)
    p.add_argument("--crawler", help="also wait for this crawler (started by the Glue trigger)")
    p.add_argument("--poll-seconds", type=int, default=30)
    a = p.parse_args()

    glue = boto3.client("glue")
    run_id = glue.start_job_run(JobName=a.job, Arguments={"--date": a.date})["JobRunId"]
    print(f"started {a.job} run {run_id}", flush=True)
    while True:
        run = glue.get_job_run(JobName=a.job, RunId=run_id)["JobRun"]
        state = run["JobRunState"]
        if state in TERMINAL:
            print(f"{state} after {run.get('ExecutionTime', 0)}s {run.get('ErrorMessage', '')}", flush=True)
            if state != "SUCCEEDED":
                return 1
            break
        time.sleep(a.poll_seconds)
    return wait_for_crawler(glue, a.crawler, a.poll_seconds) if a.crawler else 0


def wait_for_crawler(glue, name: str, poll: int) -> int:
    """The conditional trigger starts the crawler a few seconds after the job succeeds."""
    started = False
    for _ in range(120):  # ~1h at 30s polls
        c = glue.get_crawler(Name=name)["Crawler"]
        if c["State"] in ("RUNNING", "STOPPING"):
            started = True
        elif started or _ > 6:  # back to READY after running (or it finished before we looked)
            status = c.get("LastCrawl", {}).get("Status")
            print(f"crawler {name}: {status}", flush=True)
            return 0 if status == "SUCCEEDED" else 1
        time.sleep(poll)
    print(f"crawler {name} did not finish in time", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
