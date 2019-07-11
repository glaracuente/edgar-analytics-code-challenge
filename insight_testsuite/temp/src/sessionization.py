#!/usr/local/bin/python3

import datetime
import os
dir = os.path.dirname(__file__)

# Get interval time and logs from relevant files
inactivity_file = os.path.join(dir, '../input/inactivity_period.txt')
with open(inactivity_file, 'r') as f:
  interval = f.read()
  f.close()

log_file = os.path.join(dir, '../input/log.csv')
with open(log_file, 'r') as f:
  log = f.read()
  f.close()

interval = int(interval)
log = log.split("\n")
headers = log[0]
del log[0]
del log[len(log)-1]

# Create dictonary for headers
column = 0
cols = {}

for header in headers.split(","):
    cols[header] = column
    column += 1

# Main sessionizatio logic
completed_sessions = []
open_sessions = []

for line in log:
    fields = line.split(",")
    ip = fields[cols["ip"]]
    cik = fields[cols["cik"]]
    accession = fields[cols["accession"]]
    extention = fields[cols["extention"]]
    doc = "{}{}{}".format(cik, accession, extention)
    date = fields[cols["date"]]
    time = fields[cols["time"]]
    current_datetime = "{} {}".format(date, time)
    current_datetime_obj = datetime.datetime.strptime(current_datetime, '%Y-%m-%d %H:%M:%S')

    ip_open = False
    # If this IP/User has a session open, update it
    for session_flat in open_sessions:
        session = session_flat.split(',')

        if ip in session[0]:
            ip_open = True
            start_datetime = session[1]
            start_datetime_obj = datetime.datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')

            duration = int((current_datetime_obj - start_datetime_obj).total_seconds()) + 1
            session[2] = str(current_datetime_obj)

            doc_count = int(session[-1])
            doc_count += 1

            open_sessions[open_sessions.index(session_flat)] = "{},{},{},{},{}".format(ip, start_datetime, current_datetime_obj, duration, doc_count)
        
    # If not, open a fresh session for this IP/User
    if not ip_open:
        open_session = "{},{} {},{} {},{},{}".format(ip, date, time, date, time, 1, 1)
        open_sessions.append(open_session)

    # Close sessions that have been inactive for the inactivity threshold 
    for session_flat in open_sessions:
        session = session_flat.split(',')

        last_datetime = session[2]
        last_datetime_obj = datetime.datetime.strptime(last_datetime, '%Y-%m-%d %H:%M:%S')

        duration = int((current_datetime_obj - last_datetime_obj).total_seconds())

        if duration > interval:
            completed_sessions.append(session_flat)
            open_sessions.remove(session_flat)

    # Close out remaining sessions once we reach end of the log
    if log.index(line) == len(log) - 1:
        for session_flat in open_sessions:
            completed_sessions.append(session_flat)

sesh_file = os.path.join(dir, '../output/sessionization.txt')
with open(sesh_file, 'w') as f:
    for session in completed_sessions:
        f.write("{}\n".format(session))
    f.close()