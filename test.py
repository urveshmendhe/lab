import re
from datetime import datetime, timezone, timedelta

# /home/umendhe/git/lab/test.py

logs = [
    '2025-12-01T10:30:33.182Z | 08504740 | 2025-12-0] 10:27:33 INFO foo lamba string - Successfully processed file filename_01 source foo source',
    '2025-12-01T09:29:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Processing file filename_01 attemp #0',
    '2025-12-02110: 27:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Successfully processed file filename_02 source foo source',
    '2025-12-02T09:29:33.182Z | 08504740 | 2025-12-01 10:27:33 INFO foo lamba string - Processing file filename_02 attemp #0',
]

# regex to find ISO-like timestamp fragments
ISO_RX = re.compile(r'\d{4}-\d{2}-\d{2}[T ]\d{1,2}:\s*\d{2}:\s*\d{2}(?:\.\d+)?Z?')

# regex to find processing / success messages and filename
EVENT_RX = re.compile(r'\b(Processing|Successfully processed)\s+file\s+([^\s]+)', re.I)


def try_parse_timestamp(log_line: str) -> datetime:
    """
    Try several heuristics to extract and parse a timestamp from the log line.
    Returns an aware datetime (UTC).
    """
    # first try the part before the first pipe (assumed timestamp field)
    first_field = log_line.split('|', 1)[0].strip()
    candidates = [first_field]

    # add any ISO-like matches found anywhere in the line
    candidates += ISO_RX.findall(log_line)

    for cand in candidates:
        s = cand.strip()
        # normalize: replace T with space, Z -> +00:00, remove stray spaces after colons
        s = s.replace('T', ' ')
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        s = re.sub(r':\s+', ':', s)
        # if date and hour were concatenated like 2025-12-02110:..., insert space
        s = re.sub(r'^(\d{4}-\d{2}-\d{2})(\d{2}:)', r'\1 \2', s)
        # some lines may miss seconds; skip those candidates if parse fails
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    raise ValueError(f"Could not parse timestamp from: {log_line!r}")


def extract_events(log_lines):
    """
    Returns dict: filename -> list of (timestamp, event_type) sorted by timestamp.
    event_type is 'processing' or 'success'.
    """
    events = {}
    for line in log_lines:
        try:
            ts = try_parse_timestamp(line)
        except ValueError:
            # skip lines with no parsable timestamp
            continue
        for m in EVENT_RX.finditer(line):
            action, filename = m.group(1).lower(), m.group(2)
            etype = 'processing' if 'process' in action and action.startswith('processing') else 'success'
            events.setdefault(filename, []).append((ts, etype))
    # sort per file
    for fname in events:
        events[fname].sort(key=lambda x: x[0])
    return events


def compute_durations(events):
    """
    For each filename compute list of (success_timestamp, duration) between a processing and the next success.
    Returns dict: filename -> list of (success_ts, timedelta)
    """
    results = {}
    for fname, evs in events.items():
        out = []
        last_processing = None
        for ts, etype in evs:
            if etype == 'processing':
                last_processing = ts
            elif etype == 'success':
                if last_processing and ts >= last_processing:
                    out.append((ts, ts - last_processing))
                    last_processing = None
                else:
                    # success without a prior processing (or earlier processing already matched) -> ignore
                    pass
        results[fname] = out
    return results


if __name__ == "__main__":
    events = extract_events(logs)
    durations = compute_durations(events)

    # print results in the requested format: filename | timestamp | duration
    for fname, entries in durations.items():
        for success_ts, delta in entries:
            # print exactly: filename | timestamp | duration
            print(f"{fname} | {success_ts.isoformat()} | {str(delta)}")
