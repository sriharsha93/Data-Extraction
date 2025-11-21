# scraper.py
import re
import json
import os
import time
from datetime import datetime
import pandas as pd
from dateutil import parser
from playwright.sync_api import sync_playwright

# Required keys (same as your original)
REQUIRED_KEYS = {
    'wagonX','wagonY','wagonZone','pitchLine','pitchLength',
    'shotType','shotControl','inningNumber','oversUnique','oversActual',
    'overNumber','ballNumber','totalRuns','batsmanRuns','isFour',
    'isSix','isWicket','byes','legbyes','wides','noballs',
    'penalties','timestamp','batsmanPlayerId','nonStrikerPlayerId',
    'bowlerPlayerId','totalInningRuns','totalInningWickets'
}

def extract_valid_entries(obj):
    valid_entries = []
    if isinstance(obj, dict):
        if REQUIRED_KEYS.issubset(set(obj.keys())):
            valid_entries.append(obj)
        for v in obj.values():
            valid_entries.extend(extract_valid_entries(v))
    elif isinstance(obj, list):
        for item in obj:
            valid_entries.extend(extract_valid_entries(item))
    return valid_entries

def extract_object_id_mapping(obj):
    mapping = {}
    if isinstance(obj, dict):
        if "id" in obj and "objectId" in obj:
            mapping[obj["id"]] = {
                "objectId": obj.get("objectId"),
                "name": obj.get("name") or obj.get("fullName") or obj.get("shortName")
            }
        if "player" in obj and isinstance(obj["player"], dict):
            p = obj["player"]
            if "id" in p and "objectId" in p:
                mapping[p["id"]] = {
                    "objectId": p.get("objectId"),
                    "name": p.get("name") or p.get("fullName") or p.get("shortName")
                }
        for v in obj.values():
            mapping.update(extract_object_id_mapping(v))
    elif isinstance(obj, list):
        for x in obj:
            mapping.update(extract_object_id_mapping(x))
    return mapping

def scrape_once(url):
    """Load page, extract __NEXT_DATA__ JSON, return DataFrame of valid entries (may be empty)"""
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"], headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)  # small wait to let JS render the script tag
        html = page.content()
        browser.close()

    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, flags=re.S)
    if not match:
        return pd.DataFrame(), None

    try:
        data = json.loads(match.group(1))
    except Exception:
        return pd.DataFrame(), None

    entries = extract_valid_entries(data)
    if not entries:
        return pd.DataFrame(), data

    df = pd.DataFrame(entries)
    player_map = extract_object_id_mapping(data)

    role_map = {
        "batsman": "batsmanPlayerId",
        "nonStriker": "nonStrikerPlayerId",
        "bowler": "bowlerPlayerId",
        "outPlayer": "outPlayerId"
    }

    for role, pid_col in role_map.items():
        if pid_col in df.columns:
            df[f"{role}ObjectId"] = df[pid_col].map(lambda pid: player_map.get(pid, {}).get("objectId"))
            df[f"{role}Name"] = df[pid_col].map(lambda pid: player_map.get(pid, {}).get("name"))
        else:
            df[f"{role}ObjectId"] = None
            df[f"{role}Name"] = None

    df['readable_time'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce', utc=True)
    df['scrape_time'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return df, data

def append_and_save(match_id, df_new, output_dir="match_data"):
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, f"{match_id}.csv")
    if os.path.exists(out_file):
        df_final = pd.read_csv(out_file)
    else:
        df_final = pd.DataFrame()

    if df_new.empty:
        # nothing to add
        return False

    df_combined = pd.concat([df_final, df_new], ignore_index=True)
    # drop duplicates based on inning/over/ball if present
    drop_cols = [c for c in ["inningNumber","overNumber","ballNumber"] if c in df_combined.columns]
    if drop_cols:
        df_combined.drop_duplicates(subset=drop_cols, keep="last", inplace=True)
    else:
        df_combined.drop_duplicates(keep="last", inplace=True)

    df_combined.to_csv(out_file, index=False)
    return True

if __name__ == "__main__":
    # module usage: import and call scrape_once / append_and_save from orchestrator
    pass
