"""An unfinished version of calculating runtime from sso_run_log.xml files using pandas.

"""

"""
import numpy as np
import pandas as pd
import os
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timedelta
from datetime import date

sso_run_log_dirs = ['/home/demoscnzzh/logs2',
                    '/home/demoscnzzh/logs3']
use_long_schedule_name = False # Set to True when schedule files are not in the same location.
datetime_format = '%Y%m%d:%H:%M:%S'
date_format = '%Y%m%d'
sso_run_log_csv = 'sso_run_log.csv'
columns = ['log_full_path',
           'sched_name', 
           'logdate', 
           'job_name', 
           'start_time', 
           'end_time', 
           'runtime']
init_days = 180
normal_days = 8
if os.path.exists(sso_run_log_csv):
    days = normal_days 
    sso_run_log = pd.read_csv(sso_run_log_csv)
    sso_run_log = sso_run_log.set_index('log_full_path')
else: 
    days = init_days
    sso_run_log = pd.DataFrame(columns=columns)
    sso_run_log = sso_run_log.set_index('log_full_path')

cmd = 'find ' + ' '.join(sso_run_log_dirs) + ' -name sso_run_log.xml ! -size 0 -mtime -' + str(days)
cmd_list = cmd.split()
proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
output = proc.stdout.read().decode('utf-8')
sso_run_log_files = output.split()

for log_file in sso_run_log_files:
    try:
        sso_run_log = sso_run_log.drop([log_file])
    except(KeyError):
        pass
    tree = ET.parse(log_file)
    root = tree.getroot()
    sched_name = root.get('schedule') if use_long_schedule_name else os.path.basename(root.get('schedule'))
    logdate = root.get('logdate')
    entries = tree.findall('entry')
    for entry in entries:
        e_errors = entry.get('errors')
        if int(e_errors) == 0:
            job_name = entry.get('name')
            start_time = entry.get('start_time')
            end_time = entry.get('end_time')
            runtime = entry.get('elapsed')
            df = pd.DataFrame(columns=columns)
            df = df.set_index('log_full_path')
            df.loc[log_file] = [sched_name,  
                                logdate, 
                                job_name, 
                                start_time, 
                                end_time, 
                                runtime]
            sso_run_log = sso_run_log.append(df)
sso_run_log.to_csv(sso_run_log_csv)

sso_run_log['logdate'] = pd.to_datetime(sso_run_log['logdate'], format=date_format)
sso_run_log['start_time'] = pd.to_datetime(sso_run_log['start_time'], format=datetime_format)
sso_run_log['end_time'] = pd.to_datetime(sso_run_log['end_time'], format=datetime_format)
sso_run_log['runtime'] = pd.to_timedelta(sso_run_log['runtime'])
sso_run_log = sso_run_log[(sso_run_log['logdate'] > pd.Timestamp(date.today() - timedelta(weeks=6, days=1))) &
                          (sso_run_log['logdate'] < pd.Timestamp(date.today() - timedelta(days=1)))]
sched_runtime = sso_run_log.reset_index().set_index(['log_full_path', 'sched_name', 'logdate'])
sched_runtime = sched_runtime.drop(['job_name', 'runtime'], axis=1)
sched_runtime_grouping = sched_runtime.groupby(level=['log_full_path', 'sched_name', 'logdate'])
sched_runtime_max_end = sched_runtime_grouping.agg(max)['end_time']
sched_runtime_min_start = sched_runtime_grouping.agg(min)['start_time']
sched_runtime = pd.DataFrame(sched_runtime_max_end - sched_runtime_min_start, columns=['runtime'])
sched_runtime = sched_runtime.reset_index()
sched_runtime['runtime'] = sched_runtime['runtime'].values.astype(np.int64)
sched_runtime_daily = sched_runtime[(sched_runtime['logdate'] > pd.Timestamp(date.today() - timedelta(days=7))) &
                                    (sched_runtime['logdate'] < pd.Timestamp(date.today() - timedelta(days=1)))]
sched_runtime_daily.pop('log_full_path')
sched_runtime_daily = sched_runtime_daily.set_index(['sched_name', 'logdate'])
sched_runtime_daily = sched_runtime_daily.groupby(level=['sched_name', 'logdate']).agg(np.mean)
sched_runtime_daily['runtime'] = sched_runtime_daily['runtime'].apply(lambda v: timedelta(microseconds=v/1000))
"""