# run_schedule.py
import csv
import os
import time
import multiprocessing as mp
from datetime import datetime, timezone
from dateutil import parser, tz
import pandas as pd
from scraper import scrape_once, append_and_save

SCHEDULE_PATH = os.environ.get("SCHEDULE_PATH", "schedules/match_schedule.csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "match_data")
DEFAULT_INTERVAL = 60  # seconds between scrapes if not specified
STATUS_DIR = os.environ.get("STATUS_DIR", "status")

# IST timezone object
IST = tz.gettz("Asia/Kolkata")

def load_schedule(path):
    if not os.path.exists(path):
        print("Schedule file not found:", path)
        return []
    df = pd.read_csv(path)
    return df.to_dict(orient="records")

def parse_time_guess_ist(timestr):
    """
    Parse ISO-ish string. If tzinfo missing, assume IST.
    Returns an aware datetime.
    """
    try:
        dt = parser.isoparse(str(timestr))
    except Exception as e:
        raise
    if dt.tzinfo is None:
        # assume IST
        dt = dt.replace(tzinfo=IST)
    return dt

def is_active(match_row, now_utc):
    """Return True if now_utc is within match start/end. Handles IST naive times."""
    try:
        start_raw = match_row.get("start_time")
        end_raw = match_row.get("end_time")
        start = parse_time_guess_ist(start_raw)
        end = parse_time_guess_ist(end_raw)
    except Exception as e:
        print(f"Invalid times for {match_row.get('match_id')}: {e}")
        return False

    # convert both to UTC for comparison
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    return (start_utc <= now_utc) and (now_utc <= end_utc)

def worker_task(match_row):
    match_id = str(match_row.get("match_id"))
    url = match_row.get("url")
    interval = int(match_row.get("interval_seconds") or DEFAULT_INTERVAL)
    print(f"[{match_id}] Worker started for URL {url} (interval {interval}s)")

    try:
        end = parse_time_guess_ist(match_row.get("end_time"))
        end_utc = end.astimezone(timezone.utc)
    except Exception as e:
        print(f"[{match_id}] Invalid end_time: {e}; exiting worker.")
        return

    while datetime.now(timezone.utc) <= end_utc:
        try:
            df_new, data = scrape_once(url)
            saved = append_and_save(match_id, df_new, OUTPUT_DIR)
            print(f"[{match_id}] Scraped rows={len(df_new)} saved={saved} time={datetime.utcnow().isoformat()}")
        except Exception as e:
            print(f"[{match_id}] Error during scrape: {e}")
        time.sleep(interval)

    print(f"[{match_id}] Match window ended; worker exiting.")
    return

def summarize_status(schedule, now_utc):
    """
    Build summary dictionary:
      - next_match (by start time > now)
      - active_matches list
      - last_scrape_time (UTC now)
      - rows per match (from CSVs)
    """
    out = {}
    # normalize schedule times into aware datetimes
    entries = []
    for r in schedule:
        try:
            start = parse_time_guess_ist(r.get("start_time"))
            end = parse_time_guess_ist(r.get("end_time"))
            r["_start_dt"] = start
            r["_end_dt"] = end
            entries.append(r)
        except Exception:
            continue

    # next match (start > now)
    future = [r for r in entries if r["_start_dt"].astimezone(timezone.utc) > now_utc]
    future_sorted = sorted(future, key=lambda x: x["_start_dt"])
    if future_sorted:
        nm = future_sorted[0]
        out["next_match"] = {
            "match_id": nm.get("match_id"),
            "start_time_ist": nm["_start_dt"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "url": nm.get("url")
        }
    else:
        out["next_match"] = None

    # active matches
    active = [r for r in entries if is_active(r, now_utc)]
    active_list = []
    for a in active:
        mid = str(a.get("match_id"))
        csv_path = os.path.join(OUTPUT_DIR, f"{mid}.csv")
        rows = 0
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                rows = len(df)
            except Exception:
                rows = 0
        active_list.append({
            "match_id": mid,
            "start_time_ist": a["_start_dt"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "end_time_ist": a["_end_dt"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "url": a.get("url"),
            "rows": rows
        })
    out["active_matches"] = active_list

    out["last_scrape_time_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    out["generated_at_ist"] = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S %Z")
    return out

def write_status_files(status, out_dir=STATUS_DIR):
    os.makedirs(out_dir, exist_ok=True)
    json_path = os.path.join(out_dir, "status.json")
    html_path = os.path.join(out_dir, "status.html")

    import json
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2, ensure_ascii=False)

    # Simple HTML dashboard
    rows_html = ""
    for m in status.get("active_matches", []):
        rows_html += f"""
        <tr>
          <td>{m['match_id']}</td>
          <td><a href="{m['url']}">link</a></td>
          <td>{m['start_time_ist']}</td>
          <td>{m['end_time_ist']}</td>
          <td>{m['rows']}</td>
        </tr>
        """

    next_match_html = "None"
    nm = status.get("next_match")
    if nm:
        next_match_html = f"{nm['match_id']} ({nm['start_time_ist']}) <a href=\"{nm['url']}\">link</a>"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Scraper Status</title>
  <style>
    body{{font-family:Arial,Helvetica,sans-serif;padding:18px}}
    table{{border-collapse:collapse;width:100%;max-width:900px}}
    th,td{{border:1px solid #ddd;padding:8px;text-align:left}}
    th{{background:#f3f3f3}}
  </style>
</head>
<body>
  <h2>Match Scraper Status</h2>
  <p><strong>Generated:</strong> {status.get('generated_at_ist')}</p>
  <p><strong>Last scrape (UTC):</strong> {status.get('last_scrape_time_utc')}</p>
  <p><strong>Next match:</strong> {next_match_html}</p>

  <h3>Active matches</h3>
  <table>
    <thead>
      <tr><th>Match ID</th><th>URL</th><th>Start (IST)</th><th>End (IST)</th><th>Rows scraped</th></tr>
    </thead>
    <tbody>
      {rows_html if rows_html else '<tr><td colspan="5">No active matches</td></tr>'}
    </tbody>
  </table>
</body>
</html>
"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print("Wrote status:", json_path, html_path)

def update_readme_section(status, readme_path="README.md"):
    """
    Replace content between markers:
    <!--SCRAPER_STATUS_START--> ... <!--SCRAPER_STATUS_END-->
    """
    marker_start = "<!--SCRAPER_STATUS_START-->"
    marker_end = "<!--SCRAPER_STATUS_END-->"
    table_rows = ""
    for m in status.get("active_matches", []):
        table_rows += f"| {m['match_id']} | [{m['url']}]({m['url']}) | {m['start_time_ist']} | {m['rows']} |\n"

    if not table_rows:
        table_rows = "| - | - | - | - |\n"

    next_match = status.get("next_match")
    next_line = "None"
    if next_match:
        next_line = f"{next_match['match_id']} ({next_match['start_time_ist']})"

    new_block = f"""
{marker_start}
### Scraper status (auto-generated)
**Generated (IST):** {status.get('generated_at_ist')}  
**Last scrape (UTC):** {status.get('last_scrape_time_utc')}  

**Next match:** {next_line}

**Active matches and rows scraped:**  

| Match ID | URL | Start (IST) | Rows |
|---:|---|---|---|
{table_rows}
{marker_end}
""".strip()

    # create README.md if missing
    if not os.path.exists(readme_path):
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(new_block + "\n")
        return

    with open(readme_path, "r", encoding="utf-8") as f:
        content = f.read()

    if marker_start in content and marker_end in content:
        pre = content.split(marker_start)[0]
        post = content.split(marker_end)[1]
        updated = pre + new_block + post
    else:
        # append at end
        updated = content + "\n\n" + new_block

    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(updated)

def main():
    now_utc = datetime.now(timezone.utc)
    schedule = load_schedule(SCHEDULE_PATH)
    # spawn active workers
    active = [row for row in schedule if is_active(row, now_utc)]

    if active:
        procs = []
        for row in active:
            p = mp.Process(target=worker_task, args=(row,))
            p.start()
            procs.append(p)
        # join: allow workers to run until they finish
        for p in procs:
            p.join()
    else:
        print("No active matches at", now_utc.isoformat())

    # After one run spawn status files
    status = summarize_status(schedule, now_utc)
    write_status_files(status, STATUS_DIR)
    update_readme_section(status)
    print("Status updated.")

if __name__ == "__main__":
    main()
