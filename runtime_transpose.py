"""An unfinished version of calculating runtime from sso_run_log.xml files in which I wrote a transpose function.

"""

"""
import os
import sqlite3
import subprocess
import xml.etree.ElementTree as ET

sso_run_log_dirs = ['/home/demoscnzzh/logs1',
                    '/home/demoscnzzh/logs2',
                    '/home/demoscnzzh/logs3']
use_long_schedule_name = False # Set to True when schedule files are not in the same location.
datetime_format = '%Y%m%d:%H:%M:%S'
date_format = '%Y%m%d'
sso_runtime_db = 'sso_runtime.db'
init_days = 180
normal_days = 8
if os.path.exists(sso_runtime_db):
    days = normal_days
    conn = sqlite3.connect(sso_runtime_db)
else:
    days = init_days
    conn = sqlite3.connect(sso_runtime_db)
    conn.execute('''
        CREATE TABLE sso_run_log(
            log_full_path VARCHAR(500),
            s_schedule    VARCHAR(500),
            s_logdate     CHAR(8),
            e_name        VARCHAR(100),
            e_start_time  CHAR(17),
            e_end_time    CHAR(17),
            e_elapsed     CHAR(8)
        );
    ''')

cmd = 'find ' + ' '.join(sso_run_log_dirs) + ' -name sso_run_log.xml ! -size 0 -mtime -' + str(days)
cmd_list = cmd.split()
proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
output = proc.stdout.read().decode('utf-8')
sso_run_log_files = output.split()

for log_file in sso_run_log_files:
    conn.execute('DELETE FROM sso_run_log WHERE log_full_path = ?', (log_file,))
    conn.commit();

    tree = ET.parse(log_file)
    root = tree.getroot()
    s_schedule = root.get('schedule') if use_long_schedule_name else os.path.basename(root.get('schedule'))
    s_logdate = root.get('logdate')
    entries = tree.findall('entry')
    for entry in entries:
        e_errors = entry.get('errors')
        if int(e_errors) == 0:
            e_name = entry.get('name')
            e_start_time = entry.get('start_time')
            e_end_time = entry.get('end_time')
            e_elapsed = entry.get('elapsed')
            conn.execute('INSERT INTO sso_run_log VALUES(?, ?, ?, ?, ?, ?, ?)', 
                         (log_file, s_schedule, s_logdate, e_name, e_start_time, e_end_time, e_elapsed))
    conn.commit()

conn.executescript('''
    CREATE TABLE sso_runtime AS
    SELECT log_full_path, 
           s_schedule AS sched_name, 
           date(substr(s_logdate, 1, 4) || '-' || substr(s_logdate, 5, 2) || '-' || substr(s_logdate, 7, 2)) AS logdate, 
           e_name AS job_name, 
           datetime(substr(e_start_time, 1, 4) || '-' || substr(e_start_time, 5, 2) || '-' || substr(e_start_time, 7, 2) || ' ' || substr(e_start_time, -8)) AS start_time, 
           datetime(substr(e_end_time, 1, 4) || '-' || substr(e_end_time, 5, 2) || '-' || substr(e_end_time, 7, 2) || ' ' || substr(e_end_time, -8)) AS end_time,
           e_elapsed as runtime,
           (cast(substr(e_elapsed, 1, 2) AS INT) * 60 + cast(substr(e_elapsed, 4, 2) AS INT)) * 60 + cast(substr(e_elapsed, 7, 2) AS INT) AS runtime_n
      FROM sso_run_log;

    DELETE FROM sso_runtime WHERE logdate < date('now', '-42 days');

    CREATE TABLE sched_runtime AS
    SELECT DISTINCT log_full_path, 
           sched_name, 
           logdate, 
           MAX(strftime('%s', end_time)) - MIN(strftime('%s', start_time)) AS runtime
      FROM sso_runtime 
     GROUP BY log_full_path, 
              sched_name, 
              logdate;

    CREATE TABLE sched_runtime_daily AS
    SELECT DISTINCT sched_name AS name, 
           logdate, 
           cast(AVG(runtime) AS INT) AS runtime
      FROM sched_runtime 
     WHERE logdate BETWEEN date('now', '-7 days') AND date('now', '-1 days')
     GROUP BY name, 
              logdate;

    CREATE TABLE sched_runtime_weekly AS
    SELECT DISTINCT sched_name AS name, 
           date(logdate, '1 days', 'weekday 0', '-7 days') AS logdate, 
           cast(AVG(runtime) AS INT) AS runtime
      FROM sched_runtime 
     GROUP BY name, 
              logdate;

    CREATE TABLE job_runtime_daily AS
    SELECT DISTINCT job_name AS name, 
           logdate, 
           cast(AVG(runtime_n) AS INT) AS runtime
      FROM sso_runtime
     WHERE logdate BETWEEN date('now', '-7 days') AND date('now', '-1 days')
     GROUP BY name, 
              logdate;

    CREATE TABLE job_runtime_weekly AS
    SELECT DISTINCT job_name AS name, 
           date(logdate, '1 days', 'weekday 0', '-7 days') AS logdate, 
           cast(AVG(runtime_n) AS INT) AS runtime
      FROM sso_runtime 
     GROUP BY name, 
              logdate;
    ''')
conn.commit()


def transpose(conn, table):
    trans_dict = {}
    trans_name_set = set()
    trans_date_set = set()
    trans_name_list = []
    trans_date_list = []
    cursor = conn.execute("SELECT name, replace(logdate, '-', ''), runtime FROM " + table)
    for row in cursor:
        if row[0] not in trans_dict:
            trans_dict[row[0]] = {}
        trans_dict[row[0]][row[1]] = row[2]

    for key in trans_dict:
        trans_name_set.add(key)
        trans_date_set = trans_date_set | set(trans_dict[key].keys())

    trans_name_list = list(trans_name_set)
    trans_name_list.sort()
    trans_date_list = list(trans_date_set)
    trans_date_list.sort()

    conn.execute('CREATE TABLE ' + table + '_t(name VARCHAR(500), MIN INT, AVG INT, MAX INT, C' + ' INT, C'.join(trans_date_list) + ' INT)')

    for name in trans_name_list:
        v_min = min(trans_dict[name].values())
        v_avg = round(sum(trans_dict[name].values()) / len(trans_dict[name].values()))
        v_max = max(trans_dict[name].values())
        values = [trans_dict[name].get(date, 0) for date in trans_date_list]
        values_str = [str(i) for i in values]
        conn.execute('INSERT INTO ' + table + '_t VALUES("' + name + '", ' + str(v_min) + ', ' + str(v_avg) + ', ' + str(v_max) + ', ' + ', '.join(values_str) + ')')
    conn.commit()

transpose(conn, 'sched_runtime_daily')
transpose(conn, 'sched_runtime_weekly')
transpose(conn, 'job_runtime_daily')
transpose(conn, 'job_runtime_weekly')
conn.close()
"""