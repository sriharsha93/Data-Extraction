# run_schedule.py

import csv
import os
import time
from datetime import datetime, timezone
from dateutil import parser, tz
import pandas as pd

from scraper import scrape_once, append_and_save

SCHEDULE_PATH = os.environ.get("SCHEDULE_PATH", "schedules/match_schedule.csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "match_data")
DEFAULT_INTERVAL = 60  # seconds
STATUS_DIR = os.environ.get("STATUS_DIR", "status")

IST = tz.gettz("Asia/Kolkata")


# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------
def load_schedule(path):
    if not os.path.exists(path):
        print("Schedule not found:", path)
        return []
    df = pd.read_csv(path)
    return df.to_dict(orient="records")


def parse_time_guess_ist(timestr):
    """Parse ISO time; assume IST if no tzinfo."""
    dt = parser.isoparse(str(timestr))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt


def is_active(match_row, now_utc):
    """Return True if match should run at the current moment."""
    try:
        start = parse_time_guess_ist(match_row["start_time"])
        end = parse_time_guess_ist(match_row["end_time"])
    except Exception:
        return False

    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)

    return start_utc <= now_utc <= end_utc


# -----------------------------------------------------------
# Worker (now sequential — NO multiprocessing)
# -----------------------------------------------------------
def worker_task(match_row):
    match_id = str(match_row.get("match_id"))
    url = match_row.get("url")
    interval = int(match_row.get("interval_seconds") or DEFAULT_INTERVAL)

    print(f"[{match_id}] Starting scraper loop every {interval}s")
    print(f"[{match_id}] URL = {url}")

    # parse match end time
    end = parse_time_guess_ist(match_row.get("end_time"))
    end_utc = end.astimezone(timezone.utc)

    while datetime.now(timezone.utc) <= end_utc:
        try:
            df_new, data = scrape_once(url)
            saved = append_and_save(match_id, df_new, OUTPUT_DIR)
            print(
                f"[{match_id}] Scraped rows={len(df_new)} | saved={saved} | "
                f"time={datetime.utcnow().isoformat()}"
            )
        except Exception as e:
            print(f"[{match_id}] Error: {e}")

        time.sleep(interval)

    print(f"[{match_id}] Match ended, worker exiting.")


# -----------------------------------------------------------
# Status Dashboard
# -----------------------------------------------------------
def summarize_status(schedule, now_utc):
    out = {}
    entries = []

    for r in schedule:
        try:
            r["_start"] = parse_time_guess_ist(r["start_time"])
            r["_end"] = parse_time_guess_ist(r["end_time"])
            entries.append(r)
        except Exception:
            pass

    # Next match
    future = [x for x in entries if x["_start"].astimezone(timezone.utc) > now_utc]
    future = sorted(future, key=lambda r: r["_start"])
    if future:
        nm = future[0]
        out["next_match"] = {
            "match_id": nm["match_id"],
            "start_time_ist": nm["_start"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "url": nm["url"],
        }
    else:
        out["next_match"] = None

    # Active matches table
    active = [x for x in entries if is_active(x, now_utc)]
    act_list = []
    for m in active:
        mid = str(m["match_id"])
        csv_path = os.path.join(OUTPUT_DIR, f"{mid}.csv")
        rows = 0

        if os.path.exists(csv_path):
            try:
                rows = len(pd.read_csv(csv_path))
            except Exception:
                pass

        act_list.append({
            "match_id": mid,
            "url": m["url"],
            "start_time_ist": m["_start"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "end_time_ist": m["_end"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "rows": rows,
        })

    out["active_matches"] = act_list

    out["last_scrape_time_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    out["generated_at_ist"] = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S %Z")

    return out


def write_status_files(status, out_dir=STATUS_DIR):
    os.makedirs(out_dir, exist_ok=True)

    import json

    # JSON
    with open(os.path.join(out_dir, "status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    # HTML Dashboard
    active_html = ""
    for m in status["active_matches"]:
        active_html += f"""
        <tr>
            <td>{m['match_id']}</td>
            <td><a href="{m['url']}">link</a></td>
            <td>{m['start_time_ist']}</td>
            <td>{m['end_time_ist']}</td>
            <td>{m['rows']}</td>
        </tr>
        """

    nm = status["next_match"]
    nm_html = "None"
    if nm:
        nm_html = f'{nm["match_id"]} ({nm["start_time_ist"]}) <a href="{nm["url"]}">link</a>'

    html = f"""
    <html><body>
    <h2>Scraper Status</h2>
    <p><b>Generated (IST):</b> {status['generated_at_ist']}</p>
    <p><b>Last Scrape (UTC):</b> {status['last_scrape_time_utc']}</p>
    <p><b>Next match:</b> {nm_html}</p>

    <h3>Active Matches</h3>
    <table border="1" cellpadding="6">
        <tr><th>ID</th><th>URL</th><th>Start</th><th>End</th><th>Rows</th></tr>
        {active_html or '<tr><td colspan="5">None</td></tr>'}
    </table>
    </body></html>
    """

    with open(os.path.join(out_dir, "status.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print("Updated status files.")


def update_readme_section(status, readme_path="README.md"):
    start = "<!--SCRAPER_STATUS_START-->"
    end = "<!--SCRAPER_STATUS_END-->"

    rows = ""
    for m in status["active_matches"]:
        rows += f"| {m['match_id']} | [{m['url']}]({m['url']}) | {m['start_time_ist']} | {m['rows']} |\n"

    if not rows:
        rows = "| - | - | - | - |\n"

    nm = status["next_match"]
    next_line = "None" if not nm else f"{nm['match_id']} ({nm['start_time_ist']})"

    block = f"""
{start}
### Scraper status
Generated (IST): **{status['generated_at_ist']}**  
Last scrape (UTC): **{status['last_scrape_time_utc']}**

**Next Match:** {next_line}

| Match ID | URL | Start (IST) | Rows |
|---|---|---|---|
{rows}
{end}
""".strip()

    if not os.path.exists(readme_path):
        with open(readme_path, "w") as f:
            f.write(block)
        return

    txt = open(readme_path, "r", encoding="utf-8").read()

    if start in txt and end in txt:
        before = txt.split(start)[0]
        after = txt.split(end)[1]
        new_txt = before + block + after
    else:
        new_txt = txt + "\n\n" + block

    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(new_txt)


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
def main():
    now_utc = datetime.now(timezone.utc)
    schedule = load_schedule(SCHEDULE_PATH)

    # Run workers sequentially (GitHub Actions-safe)
    active = [r for r in schedule if is_active(r, now_utc)]

    if active:
        for row in active:
            print(f"\n===== Running worker for match {row.get('match_id')} =====")
            worker_task(row)
    else:
        print("No active matches at", now_utc.isoformat())

    # Update status files
    status = summarize_status(schedule, now_utc)
    write_status_files(status)
    update_readme_section(status)
    print("Status updated.")


if __name__ == "__main__":
    main()
