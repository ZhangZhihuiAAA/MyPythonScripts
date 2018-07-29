"""Calculate runtime from sso_run_log.xml files.

"""

"""
import os
import sys
import sqlite3
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime as DT
import logging
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from xml.etree.ElementTree import ParseError
from xml.parsers.expat import ExpatError


TLA = 'ZZH'
ENV = 'PROD'
level_1 = 'Schedule'
level_2 = 'Job'
sso_run_log_dirs = ['/home/demoscnzzh/logs1', 
                    '/home/demoscnzzh/logs2', 
                    '/home/demoscnzzh/logs3']
use_long_schedule_name = False # Set to True when schedule files are not in the same location.
init_days = 180
normal_days = 8
home_dir = os.path.dirname(__file__)
os.chdir(home_dir)
sso_runtime_db = 'sso_runtime.db'
datetime_format = '%Y%m%d:%H:%M:%S'
datetime_format2 = '%Y-%m-%d %H:%M:%S'
date_format = '%Y%m%d'
sched_rpt = ENV.lower() + '_schedule_runtime.html'
sched_table1 = 'sched_table1.html'
sched_table2 = 'sched_table2.html'
job_rpt = ENV.lower() + '_job_runtime.html'
job_table1 = 'job_table1.html'
job_table2 = 'job_table2.html'
temp = 'template.html'
email_server = 'mail.sas.com'
email_from = 'replies-disabled@sas.com'
email_to = ['zhihui.zhang@sas.com']
email_subject = TLA + ' ' + ENV + ' Runtime Reports - ' + DT.now().strftime('%m/%d/%Y')
email_body = 'Please see the attachments.'
email_attachment = [sched_rpt, job_rpt]
log_file = 'ssorun_runtime_report.log'

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
logger.addHandler(handler)

logger.info("Starting stage 1 at " + DT.now().strftime(datetime_format2))

if os.path.exists(sso_runtime_db):
    days = normal_days
    conn = sqlite3.connect(sso_runtime_db)
else:
    days = init_days
    conn = sqlite3.connect(sso_runtime_db)
    conn.execute('''
        CREATE TABLE sso_run_log(
            log_full_path TEXT,
            s_schedule    TEXT,
            s_logdate     CHAR(8),
            e_name        TEXT,
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

logger.info("Starting stage 2 at " + DT.now().strftime(datetime_format2))

for log_file in sso_run_log_files:
    conn.execute('DELETE FROM sso_run_log WHERE log_full_path = ?', (log_file,))
    conn.commit();

    try:
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
                try:
                    e_elapsed = entry.get('elapsed')
                except ExpatError:
                    e_elapsed = '00:00:00'
                conn.execute('INSERT INTO sso_run_log VALUES(?, ?, ?, ?, ?, ?, ?)', 
                             (log_file, s_schedule, s_logdate, e_name, e_start_time, e_end_time, e_elapsed))
        conn.commit()
    except ParseError:
        pass

logger.info("Starting stage 3 at " + DT.now().strftime(datetime_format2))

try:
    conn.executescript('''
        DROP TABLE sso_runtime;
        DROP TABLE sched_runtime;
        DROP TABLE sched_runtime_daily;
        DROP TABLE sched_runtime_weekly;
        DROP TABLE job_runtime_daily;
        DROP TABLE job_runtime_weekly;
        ''')
    conn.commit()
except sqlite3.OperationalError:
    pass

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

logger.info("Starting stage 4 at " + DT.now().strftime(datetime_format2))

def seconds_to_timeinterval(seconds):
    if seconds is None or seconds == 0:
        return '.'
    else:
        h = seconds // 3600
        m = seconds % 3600 // 60
        s = seconds % 60
        return "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)


def create_table_head(head):
    print('<table class="table" cellspacing="1" cellpadding="7" rules="groups" frame="box" border="1" bordercolor="#000000" summary="Procedure Print">')
    print('<colgroup>')
    print('<col>')
    print('</colgroup>')
    print('<colgroup>')
    for i in range(len(head)):
        print('<col>')
    print('</colgroup>')
    print('<thead>')
    print('<tr>')
    print('<th class="r header" scope="col">Obs</th>')
    for i in range(len(head)):
        if i == 0:
            print('<th class="l header" scope="col">' + head[i] + '</th>')
        else:
            print('<th class="r header" scope="col">' + head[i] + '</th>')
    print('</tr>')
    print('</thead>')
    print('<tbody>')


def insert_table_row(obs, row):
    print('<tr>')
    print('<th class="r rowheader" scope="row">' + str(obs) + '</th>')
    for i in range(len(row)):
        if i == 0:
            print('<td class="l data">' + row[i] + '</td>')
        else:
            print('<td class="r data">' + seconds_to_timeinterval(row[i]) + '</td>')
    print('</tr>')


def add_table_foot():
    print('</tbody>')
    print('</table>')


def create_table_html(level, table):
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

    create_table_head([level] + trans_date_list + ['MIN', 'MAX', 'AVG'])

    row_num = 0
    for name in trans_name_list:
        row_num += 1
        zlist = [x for x in trans_dict[name].values() if x is not None]
        if len(zlist) > 0:
            v_min = min(zlist)
            v_avg = round(sum(zlist) / len(zlist))
            v_max = max(zlist)
        else:
            v_min = 0
            v_avg = 0
            v_max = 0
        values = [trans_dict[name].get(date, 0) for date in trans_date_list]
        insert_table_row(row_num, [name] + values + [v_min, v_max, v_avg])

    add_table_foot()


logger.info("Starting stage 5 at " + DT.now().strftime(datetime_format2))

output = sys.stdout
sys.stdout = open(sched_table1, 'w')
create_table_html(level_1, 'sched_runtime_daily')
sys.stdout = open(sched_table2, 'w')
create_table_html(level_1, 'sched_runtime_weekly')
sys.stdout = open(job_table1, 'w')
create_table_html(level_2, 'job_runtime_daily')
sys.stdout = open(job_table2, 'w')
create_table_html(level_2, 'job_runtime_weekly')
sys.stdout = output

conn.close()

logger.info("Starting stage 6 at " + DT.now().strftime(datetime_format2))

def replace_mark(temp_file, mark, content, result_file):
    f1 = open(temp_file, 'r')
    str_f1 = f1.read()
    f1.close()
    try:
        f2 = open(content, 'r')
        str_f2 = f2.read()
        f2.close()
    except FileNotFoundError:
        str_f2 = content
    f3 = open(result_file, 'w')
    f3.write(str_f1.replace(mark, str_f2))
    f3.close()


replace_mark(temp, '{TABLE1}', sched_table1, sched_rpt)
replace_mark(sched_rpt, '{TABLE2}', sched_table2, sched_rpt)
replace_mark(sched_rpt, '{TLA}', TLA, sched_rpt)
replace_mark(sched_rpt, '{ENV}', ENV, sched_rpt)
replace_mark(sched_rpt, '{LEVEL}', level_1, sched_rpt)
replace_mark(sched_rpt, '{RPT_TIME}', DT.now().strftime(datetime_format2), sched_rpt)
replace_mark(temp, '{TABLE1}', job_table1, job_rpt)
replace_mark(job_rpt, '{TABLE2}', job_table2, job_rpt)
replace_mark(job_rpt, '{TLA}', TLA, job_rpt)
replace_mark(job_rpt, '{ENV}', ENV, job_rpt)
replace_mark(job_rpt, '{LEVEL}', level_2, job_rpt)
replace_mark(job_rpt, '{RPT_TIME}', DT.now().strftime(datetime_format2), job_rpt)
os.remove(sched_table1)
os.remove(sched_table2)
os.remove(job_table1)
os.remove(job_table2)

logger.info("Starting stage 7 at " + DT.now().strftime(datetime_format2))

def send_mail(server, send_from, send_to, subject, text, files=None):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)


    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

send_mail(email_server, email_from, email_to, email_subject, email_body, files=email_attachment)

logger.info("All done at " + DT.now().strftime(datetime_format2))
"""