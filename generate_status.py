# generate_status.py
from run_schedule import load_schedule, summarize_status, write_status_files, update_readme_section
from datetime import datetime, timezone
from dateutil import tz

def main():
    sched = load_schedule("schedules/match_schedule.csv")
    now_utc = datetime.now(timezone.utc)
    status = summarize_status(sched, now_utc)
    write_status_files(status, "status")
    update_readme_section(status)
    print("Status generated.")

if __name__ == "__main__":
    main()
