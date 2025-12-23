from datetime import datetime, timezone
import re

# /home/umendhe/git/lab/test.py

logs = [
'2025-12-01T10:27:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Successfully processed file filename_01 source foo source',
'2025-12-01T09:29:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Processing file filename_01 attemp #0',
'2025-12-02110:27:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Successfully processed file filename_02 source foo source',
'2025-12-02T09:29:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Processing file filename_02 attemp #0'
]

# helper: robust timestamp parser (best-effort)
def parse_timestamp(raw_ts):
    s = raw_ts.strip()
    # keep only typical timestamp characters
    s = re.sub(r'[^\dTt Zz:\-\.+]', '', s)
    # normalize space -> T between date and time if present
    s = re.sub(r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}:\d{2})', r'\1T\2', s)
    # convert trailing Z to +00:00 for fromisoformat
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    # try fromisoformat
    for attempt in (s, s.replace('T', ' '), s.replace('T', '')):
        try:
            dt = datetime.fromisoformat(attempt)
            # make timezone-aware UTC if naive
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    # fallback: try to extract date and time groups manually
    m = re.search(r'(\d{4}-\d{2}-\d{2}).*?(\d{1,2}:\d{2}:\d{2}(?:\.\d+)?)', raw_ts)
    if m:
        datepart, timepart = m.groups()
        candidate = f"{datepart}T{timepart}"
        if candidate.endswith('Z'):
            candidate = candidate[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    raise ValueError(f"unparseable timestamp: {raw_ts!r}")

# parse log lines into structured records
records = []
for line in logs:
    parts = [p.strip() for p in line.split('|', 2)]
    if len(parts) < 3:
        continue
    ts_raw, aws_id, msg = parts
    try:
        ts = parse_timestamp(ts_raw)
    except ValueError:
        # skip unparseable timestamp lines
        continue
    records.append({"ts": ts, "aws": aws_id, "msg": msg})

# sort by timestamp just in case
records.sort(key=lambda r: r["ts"])

# detect events and compute durations
start_re = re.compile(r'Processing file (\S+)', re.IGNORECASE)
success_re = re.compile(r'Successfully processed file (\S+)', re.IGNORECASE)

pending = {}   # filename -> list of (start_ts, aws_id)
results = []   # tuples (filename, start_ts, aws_id, duration_timedelta)

for r in records:
    msg = r["msg"]
    ts = r["ts"]
    aws = r["aws"]
    m_start = start_re.search(msg)
    if m_start:
        fname = m_start.group(1)
        pending.setdefault(fname, []).append((ts, aws))
        continue
    m_succ = success_re.search(msg)
    if m_succ:
        fname = m_succ.group(1)
        if fname in pending and pending[fname]:
            start_ts, start_aws = pending[fname].pop(0)  # FIFO matching
            duration = ts - start_ts
            results.append((fname, start_ts, start_aws, duration))
        else:
            # unmatched success; ignore or record with None start
            results.append((fname, None, aws, None))

# print results in requested format: "filename | timestamp | aws_ac_id | duration"
for fname, start_ts, aws, duration in results:
    ts_str = start_ts.isoformat() if start_ts else "N/A"
    dur_str = f"{duration.total_seconds():.3f}s" if duration is not None else "N/A"
    print(f"{fname} | {ts_str} | {aws} | {dur_str}")

# If you want this functionally reusable, you can extract into a function.