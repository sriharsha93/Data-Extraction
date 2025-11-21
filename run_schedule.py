# run_schedule.py
import csv
import os
import time
import multiprocessing as mp
from datetime import datetime, timezone
from dateutil import parser
import pandas as pd
from scraper import scrape_once, append_and_save

SCHEDULE_PATH = os.environ.get("SCHEDULE_PATH", "schedules/match_schedule.csv")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "match_data")
DEFAULT_INTERVAL = 60  # fallback seconds between scrapes for a match

def load_schedule(path):
    if not os.path.exists(path):
        print("Schedule file not found:", path)
        return []
    df = pd.read_csv(path)
    # enforce required columns: match_id,url,start_time,end_time
    return df.to_dict(orient="records")

def is_active(match_row, now_utc):
    """Assumes times in schedule file are ISO8601 UTC (ending with Z)"""
    # parse
    try:
        start = parser.isoparse(str(match_row.get("start_time")))
        end = parser.isoparse(str(match_row.get("end_time")))
    except Exception as e:
        print(f"Invalid times for {match_row.get('match_id')}: {e}")
        return False
    # ensure timezone-aware; work in UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    return (start <= now_utc) and (now_utc <= end)

def worker_task(match_row):
    match_id = str(match_row.get("match_id"))
    url = match_row.get("url")
    interval = int(match_row.get("interval_seconds") or DEFAULT_INTERVAL)
    print(f"[{match_id}] Worker started for URL {url} (interval {interval}s)")

    # run scraping loop until match end
    try:
        end_time = parser.isoparse(str(match_row.get("end_time")))
        if end_time.tzinfo is None:
            from datetime import timezone
            end_time = end_time.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"[{match_id}] Invalid end_time: {e}; exiting worker.")
        return

    while datetime.now(timezone.utc) <= end_time:
        try:
            df_new, data = scrape_once(url)
            saved = append_and_save(match_id, df_new, OUTPUT_DIR)
            print(f"[{match_id}] Scraped rows={len(df_new)} saved={saved} time={datetime.utcnow()}")
        except Exception as e:
            print(f"[{match_id}] Error during scrape: {e}")
        time.sleep(interval)

    print(f"[{match_id}] Match window ended; worker exiting.")
    return

def main():
    now = datetime.now(timezone.utc)
    schedule = load_schedule(SCHEDULE_PATH)
    active = [row for row in schedule if is_active(row, now)]

    if not active:
        print("No active matches at", now.isoformat())
        return

    # spawn a process per active match (limited by runner resources)
    procs = []
    for row in active:
        p = mp.Process(target=worker_task, args=(row,))
        p.start()
        procs.append(p)

    # wait for all to finish
    for p in procs:
        p.join()

if __name__ == "__main__":
    main()
