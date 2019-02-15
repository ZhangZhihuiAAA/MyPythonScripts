"""Track the performance of ANF batches.

"""

"""
import subprocess
import shlex
import os
import logging
import smtplib
import xml.etree.ElementTree as et
from datetime import datetime as dt
from datetime import timedelta as td
from xml.parsers.expat import ExpatError
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


daily_sched = 'anf_daily_sched_prod.xml'
friday_sched = 'anf_friday_night_sched_prod.xml'
weekly_sched = 'anf_weekly_sched_prod.xml'
maintenance_sched = 'anf_saturday_maint.xml'
archive_dir = '/sso/transport/archive'
sso_logs_dir = '/anf/projects/default/anfmi/logs'
sso_run_log = 'sso_run_log.xml'
hlm_log_dir = '/sso/sfw/LoadMgr/logs'
backup_dir = hlm_log_dir + '/backup'
etlin_daily_job = 'so_etlin_daily'
etlin_weekly_job = 'so_etlin_weekly'
plk_job = 'plk_requests'
plk_sched_job = 'plk_schedule'
plk_search_keyword = 'call_process_req_task'
plk_start_job = 'batch_plk'
plk_end_job = 'batch_plk_results'
stb_job = 'stb_requests'
stb_sched_job = 'stb_schedule'
stb_search_keyword = 'stb_call_process_req_task'
stb_start_job = 'stb_batch_input'
stb_end_job = 'stb_batch_output'
purge_job = 'so_purge'
purge_profile_log = 'purge_published_profiles.log'
purge_custom_log = 'purge_custom_data.log'
stb_results_job = 'stb_requests_results'
dir_name = os.path.dirname(os.path.abspath(__file__))
report_file = dir_name + '/ANF SO PROD batch tracker.csv'
report_file_header = 'Weekday,Date,Run Type,Time etlin finished,Time batch completed,Total runtime (hh:mm),Total runtime (minutes),PLK runtime (hh:mm),STB runtime (hh:mm),PLK volume,STB volume,Purge runtime,Purge volume of profile,Purge volume of custom data,Issue encountered'
stb_results_file = dir_name + '/ANF STB_results tracking table.csv'
stb_results_file_header = 'MODULE,LOGDATE,START_TIME,FINISH_TIME,ELAPSED_TIME'

date_format1 = '%Y%m%d'
date_format2 = '%Y-%m-%d'
date_format_rpt = '%b %d'
date_time_format1 = '%Y%m%d %H:%M:%S'
date_time_format2 = '%Y-%m-%d %H:%M:%S'
date_time_format3 = '%Y/%m/%d %H:%M:%S'
search_time_buffer = td(minutes=2)
start_date = dt.strptime('20181205', date_format1)
today = dt.today()
end_date = dt(today.year, today.month, today.day, 23, 59, 59) - td(days=2)

log_file = dir_name + '/batch_tracker.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
logger.addHandler(handler)
tmp_file = dir_name + '/tmp.txt'

email_server = 'mailhost.vsp.sas.com'
email_from = 'replies-disabled@sas.com'
email_to = ['zhihui.zhang@sas.com']
email_subject = 'ANF SO PROD batch tracker - {0}'.format(dt.strftime(end_date, date_format2))
global email_body
global email_body_zip
email_body = ''
email_body_zip = []
email_attachment = [report_file, stb_results_file]

init_run = False

class Tools():
    @staticmethod
    def exec_os_cmd(cmd, *args):
        if cmd and args:
            logger.info('CALL: exec_os_cmd was called with arguments: {0} | {1}'.format(cmd, ' | '.join(args)))
        else:
            logger.info('CALL: exec_os_cmd was called with arguments: {0}'.format(cmd))

        if cmd:
            procs = list(range(len(args) + 1))
            procs[0] = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
            if not args:
                return procs[0].stdout.read().rstrip()
            else:
                for i in range(len(args)):
                    procs[i+1] = subprocess.Popen(shlex.split(args[i]), stdin = procs[i].stdout, stdout=subprocess.PIPE)
                return procs[-1].stdout.read().rstrip()

    @staticmethod
    def get_timestamp(path, oldest=False):
        logger.info('CALL: get_timestamp was called with arguments: {0} and {1}'.format(path, oldest))
        if path and os.path.exists(path):
            if oldest:
                cmd = 'ls -lt --time-style=full-iso {0}'.format(path)
            else:
                cmd = 'ls -lrt --time-style=full-iso {0}'.format(path)
            output = Tools.exec_os_cmd(cmd)
            the_file = output.split('\n')[-1]
            the_time = the_file.split('.', 1)[0][-19:]
            the_time_fmt1 = dt.strftime(dt.strptime(the_time, date_time_format2), date_time_format1)
            logger.info('RETURN: get_timestamp returned {0}'.format(the_time_fmt1))
            return the_time_fmt1

    @staticmethod
    def seconds_to_timeinterval(seconds, has_seconds_part=False):
        logger.info('CALL: seconds_to_timeinterval was called with arguments: {0} and {1}'.format(seconds, has_seconds_part))
        if seconds is not None:
            h = seconds // 3600
            m = seconds % 3600 // 60
            s = seconds % 60
            if has_seconds_part:
                rv = "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)
            else:
                rv = "{0:02d}:{1:02d}".format(h, m)
            logger.info('RETURN: seconds_to_timeinterval returned {0}'.format(rv))
            return rv

    @staticmethod
    def get_log_file_by_job_name(log_dir, job_name):
        logger.info('CALL: get_log_file_by_job_name was called with arguments: {0} and {1}'.format(log_dir, job_name))
        if log_dir and job_name and os.path.exists(log_dir):
            cmd1 = '''grep -r "name='{0}'" {1}'''.format(job_name, log_dir)
            cmd2 = 'grep {0}'.format(sso_run_log)
            the_log_file = Tools.exec_os_cmd(cmd1, cmd2).split(':')[0]
            if the_log_file:
                logger.info('RETURN: get_log_file_by_job_name returned {0}'.format(the_log_file))
                return the_log_file

    @staticmethod
    def get_job_start_end_time_errors(sso_run_log, job_name):
        logger.info('CALL: get_job_start_end_time_errors was called with arguments: {0} and {1}'.format(sso_run_log, job_name))
        if sso_run_log and job_name and os.path.exists(sso_run_log):
            try:
                tree = et.parse(sso_run_log)
                entries = tree.findall('entry')
                if entries:
                    for entry in entries:
                        if entry.get('name') == job_name:
                            start_time = entry.get('start_time')
                            start_time_fmt1 = start_time.replace(':', ' ', 1)
                            end_time = entry.get('end_time')
                            end_time_fmt1 = end_time.replace(':', ' ', 1)
                            errors = entry.get('errors')
                            logger.info('RETURN: get_job_start_end_time_errors returned ({0},{1},{2})'.format(start_time_fmt1, end_time_fmt1, errors))
                            return start_time_fmt1, end_time_fmt1, errors
                    logger.info('DEBUG: {0} has no job {1}.'.format(sso_run_log, job_name))
                    logger.info('RETURN: get_job_start_end_time_errors returned ({0},{1},{2})'.format(None, None, None))
                    return None, None, None
                else:
                    logger.info('ERROR: {0} has not any entry.'.format(sso_run_log))
                    logger.info('RETURN: get_job_start_end_time_errors returned ({0},{1},{2})'.format(None, None, None))
                    return None, None, None
            except ExpatError:
                logger.info('ERROR: {0} can not be parsed.'.format(sso_run_log))
                logger.info('RETURN: get_job_start_end_time_errors returned ({0},{1},{2})'.format(None, None, None))
                return None, None, None
        else:
            logger.info('RETURN: get_job_start_end_time_errors returned ({0},{1},{2})'.format(None, None, None))
            return None, None, None

    @staticmethod
    def is_complete_run(log_dir, sched):
        if log_dir and sched:
            cmd_str = 'grep "<entry" {0}'
            cmd1 = cmd_str.format(log_dir + '/' + sched)
            cmd2 = cmd_str.format(log_dir + '/' + sso_run_log)
            entries_in_sched = Tools.exec_os_cmd(cmd1, 'wc -l')
            entries_in_log = Tools.exec_os_cmd(cmd2, 'wc -l')
            if int(entries_in_sched) - int(entries_in_log) <= 1:
                return True
            else:
                return False

    @staticmethod
    def has_errors(sso_run_log):
        if sso_run_log:
            cmd1 = '''grep "errors='1'" {0}'''.format(sso_run_log)
            cmd2 = 'wc -l'
            errors = Tools.exec_os_cmd(cmd1, cmd2)
            if int(errors) > 0:
                return True
            else:
                return False

    @staticmethod
    def job_in_log(job_name, sso_run_log):
        if job_name and sso_run_log:
            grep_result = Tools.exec_os_cmd('''grep "name='{0}'" {1}'''.format(job_name, sso_run_log))
            return True if grep_result else False

    @staticmethod
    def get_runtime(start_time_str, end_time_str):
        if start_time_str and end_time_str:
            start_time = dt.strptime(start_time_str, date_time_format1)
            end_time = dt.strptime(end_time_str, date_time_format1)
            runtime = end_time - start_time
            runtime_seconds = runtime.days * 24 * 3600 + runtime.seconds
            return Tools.seconds_to_timeinterval(runtime_seconds)

    @staticmethod
    def create_rpt_row(sched_run):
        rpt_row = [sched_run.run_weekday_rpt,
                   sched_run.run_date_str_rpt,
                   sched_run.run_type,
                   dt.strftime(dt.strptime(sched_run.time_etlin_finished, date_time_format1), date_time_format2) if sched_run.time_etlin_finished else None,
                   dt.strftime(dt.strptime(sched_run.time_batch_completed, date_time_format1), date_time_format2) if sched_run.time_batch_completed else None,
                   sched_run.total_runtime,
                   sched_run.total_runtime_minutes,
                   sched_run.plk_runtime,
                   sched_run.stb_runtime,
                   sched_run.plk_volume,
                   sched_run.stb_volume,
                   sched_run.purge_runtime,
                   sched_run.purge_volume_profile,
                   sched_run.purge_volume_custom_data,
                   sched_run.comment]
        rpt_row_no_none = ['' if v is None else v for v in rpt_row]
        return ','.join(rpt_row_no_none)

    @staticmethod
    def create_stb_results_row(sched_run):
        rpt_row = [stb_results_job,
                   dt.strftime(sched_run.run_date, date_format2),
                   dt.strftime(dt.strptime(sched_run.stb_results_start_time_str, date_time_format1), date_time_format2) if sched_run.stb_results_start_time_str else None,
                   dt.strftime(dt.strptime(sched_run.stb_results_end_time_str, date_time_format1), date_time_format2) if sched_run.stb_results_end_time_str else None,
                   sched_run.stb_results_runtime]
        rpt_row_no_none = ['' if v is None else v for v in rpt_row]
        return ','.join(rpt_row_no_none)

    @staticmethod
    def create_report(run_date):
        global email_body
        global email_body_zip
        run_date_str = dt.strftime(run_date, date_format1)
        logger.info('\n================================================================================')
        logger.info('Start processing date {0}\n'.format(run_date_str))
        run_weekday = dt.strftime(run_date, '%A')
        if run_weekday != 'Saturday':
            if run_weekday == 'Sunday':
                the_sched = weekly_sched
                run_type = 'Weekly'
            elif run_weekday == 'Friday':
                the_sched = friday_sched
                run_type = 'Daily'
            else:
                the_sched = daily_sched
                run_type = 'Daily'
            sched_run = SchedRun(run_date_str, the_sched, run_type)
            sched_run.load()
            sched_run.log_me()
            rpt_row = Tools.create_rpt_row(sched_run)
            email_body_zip = zip(report_file_header.split(','), rpt_row.split(','))
            with open(report_file, 'a') as f:
                f.write(rpt_row + '\n')
            if run_type == 'Daily':
                stb_results_row = Tools.create_stb_results_row(sched_run)
                email_body_zip.extend(zip(stb_results_file_header.split(','), stb_results_row.split(',')))
                with open(stb_results_file, 'a') as f:
                    f.write(stb_results_row + '\n')

            if run_weekday == 'Friday':
                if not sched_run.log_dir:
                    the_sched = maintenance_sched
                else:
                    the_sched = friday_sched
                run_type = 'Purge'
                sched_run = SchedRun(run_date_str, the_sched, run_type)
                sched_run.load()
                sched_run.log_me()
                rpt_row = Tools.create_rpt_row(sched_run)
                email_body_zip.extend(zip(report_file_header.split(','), rpt_row.split(',')))
                with open(report_file, 'a') as f:
                    f.write(rpt_row + '\n')
            elif run_weekday == 'Sunday':
                the_sched = daily_sched
                run_type = 'Daily'
                sched_run = SchedRun(run_date_str, the_sched, run_type)
                sched_run.load()
                sched_run.log_me()
                rpt_row = Tools.create_rpt_row(sched_run)
                email_body_zip.extend(zip(report_file_header.split(','), rpt_row.split(',')))
                with open(report_file, 'a') as f:
                    f.write(rpt_row + '\n')
                stb_results_row = Tools.create_stb_results_row(sched_run)
                email_body_zip.extend(zip(stb_results_file_header.split(','), stb_results_row.split(',')))
                with open(stb_results_file, 'a') as f:
                    f.write(stb_results_row + '\n')
        email_body = '\n'.join(['{0} - {1}'.format(item[0], item[1]) for item in email_body_zip])
        logger.info('End processing date {0}\n'.format(run_date_str))
        logger.info('================================================================================\n')

    @staticmethod
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
                part = MIMEApplication(fil.read(), Name=basename(f))
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)
        smtp = smtplib.SMTP(server)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.close()


class SchedRun():
    def __init__(self, run_date_str, sched, run_type):
        self.run_date_str = run_date_str
        self.sched = sched
        self.run_type = run_type
        self.run_date = dt.strptime(run_date_str, date_format1)
        self.run_weekday_rpt = dt.strftime(run_date, '%a')
        self.run_date_str_rpt = dt.strftime(run_date, date_format_rpt)
        self.sched_file = None
        self.log_dir = None
        self.log_file = None
        self.prev_run_sched_file = None
        self.prev_run_log_dir = None
        self.prev_run_log_file = None
        self.is_complete_run = None
        self.has_errors = None
        self.is_resume_run = None
        self.to_be_resumed = None
        self.nextday_date_str = dt.strftime(self.run_date + td(days=1), date_format1)
        self.resume_run_log_dir = None
        self.resume_run_log_file = None
        self.is_resumed = None
        self.time_etlin_finished = None
        self.time_batch_completed = None
        self.total_runtime = None
        self.total_runtime_minutes = None
        self.plk_start_job_run_log = None
        self.plk_end_job_run_log = None
        self.plk_start_time_str = None
        self.plk_start_job_errors = None
        self.plk_end_time_str = None
        self.plk_end_job_errors = None
        self.plk_runtime = None
        self.plk_volume = None
        self.stb_start_job_run_log = None
        self.stb_start_time_str = None
        self.stb_start_job_errors = None
        self.stb_end_job_run_log = None
        self.stb_end_time_str = None
        self.stb_end_job_errors = None
        self.stb_runtime = None
        self.stb_volume = None
        self.purge_start_time_str = None
        self.purge_end_time_str = None
        self.purge_errors = None
        self.purge_runtime = None
        self.purge_volume_profile = None
        self.purge_volume_custom_data = None
        self.stb_results_start_time_str = None
        self.stb_results_end_time_str = None
        self.stb_results_errors = None
        self.stb_results_runtime = None
        self.comment = None

    def _set_sched_file_6(self):
        cmd = 'find {0} -name {1}'.format(sso_logs_dir + '/' + self.run_date_str, self.sched)
        schedules = Tools.exec_os_cmd(cmd).split('\n')
        if schedules:
            a = sorted(schedules)
            self.sched_file = a[-1]
            self.log_dir = self.sched_file[:self.sched_file.rfind('/')]
            self.log_file = self.log_dir + '/' + sso_run_log if self.log_dir else None
            if len(schedules) > 1:
                self.prev_run_sched_file = a[-2]
                self.prev_run_log_dir = self.prev_run_sched_file[:self.prev_run_sched_file.rfind('/')]
                self.prev_run_log_file = self.prev_run_log_dir + '/' + sso_run_log
        else:
            self.comment = self.sched + ' DID NOT run.'

    def _set_is_complete_run(self):
        self.is_complete_run = Tools.is_complete_run(self.log_dir, self.sched)

    def _set_has_errors(self):
        self.has_errors = Tools.has_errors(self.log_file)
        if self.has_errors:
            self.comment = self.sched + ' had an error.'
            plk_requests_sched_errors = Tools.get_job_start_end_time_errors(self.log_file, plk_sched_job)[-1]
            if plk_requests_sched_errors == '1':
                self.comment = self.comment + ' Job {0} failed.'.format(plk_sched_job)
            stb_requests_sched_errors = Tools.get_job_start_end_time_errors(self.log_file, stb_sched_job)[-1]
            if stb_requests_sched_errors == '1':
                self.comment = self.comment + ' Job {0} failed.'.format(stb_sched_job)

    def _set_is_resume_run(self):
        if self.is_complete_run is False and self.has_errors is False:
            self.is_resume_run = True

    def _set_to_be_resumed(self):
        if self.is_complete_run is False and self.has_errors:
            self.to_be_resumed = True
            self.comment = self.sched + ' had a termination'

    def _set_resume_run_log_dir_3(self):
        if self.to_be_resumed:
            cmd = 'find {0} -name {1}'.format(sso_logs_dir + '/' + self.nextday_date_str, self.sched)
            schedules = Tools.exec_os_cmd(cmd).split('\n')
            if schedules:
                the_sched = min(schedules)
                if the_sched:
                    the_dir = the_sched[:the_sched.rfind('/')]
                    self.comment = self.comment + ' and was resumed.'
                    self.resume_run_log_dir = the_dir
                    self.resume_run_log_file = the_dir + '/' + sso_run_log
                    self.is_resumed = True
                else:
                    self.comment = self.comment + ' and was NOT resumed with the same schedule name. Manual check is needed.'
            else:
                self.comment = self.comment + ' and was NOT resumed.'
                self.is_resumed = False

    def _set_time_etlin_finished(self):
        if self.run_type == 'Weekly':
            job_name = etlin_weekly_job
        else:
            job_name = etlin_daily_job
        if self.log_file:
            end_time = Tools.get_job_start_end_time_errors(self.log_file, job_name)[1]
            if not end_time:
                if self.prev_run_log_file:
                    end_time = Tools.get_job_start_end_time_errors(self.prev_run_log_file, job_name)[1]
                    if not end_time:
                        if self.resume_run_log_file:
                            end_time = Tools.get_job_start_end_time_errors(self.resume_run_log_file, job_name)[1]
            self.time_etlin_finished = end_time

    def _set_time_batch_completed(self):
        if self.is_resumed:
            self.time_batch_completed = Tools.get_timestamp(self.resume_run_log_file)
        else:
            self.time_batch_completed = Tools.get_timestamp(self.log_file)

    def _set_total_runtime(self):
        if self.time_etlin_finished and self.time_batch_completed:
            start_time = dt.strptime(self.time_etlin_finished, date_time_format1)
            end_time = dt.strptime(self.time_batch_completed, date_time_format1)
            runtime = end_time - start_time
            runtime_seconds = runtime.days * 24 * 3600 + runtime.seconds
            self.total_runtime = Tools.seconds_to_timeinterval(runtime_seconds)
            self.total_runtime_minutes = str(runtime_seconds // 60)

    def _set_plk_start_job_run_log(self):
        if self.run_type != 'Purge' and self.log_dir:
            self.plk_start_job_run_log = Tools.get_log_file_by_job_name(self.log_dir, plk_start_job)
            if not self.plk_start_job_run_log:
                if self.is_resumed:
                    self.plk_start_job_run_log = Tools.get_log_file_by_job_name(self.resume_run_log_dir, plk_start_job)
                    if not self.plk_start_job_run_log:
                        if self.is_resume_run and self.prev_run_log_dir:
                            self.plk_start_job_run_log = Tools.get_log_file_by_job_name(self.prev_run_log_dir, plk_start_job)

    def _set_plk_end_job_run_log(self):
        if self.run_type != 'Purge' and self.log_dir:
            self.plk_end_job_run_log = Tools.get_log_file_by_job_name(self.log_dir, plk_end_job)
            if not self.plk_end_job_run_log:
                if self.is_resumed:
                    self.plk_end_job_run_log = Tools.get_log_file_by_job_name(self.resume_run_log_dir, plk_end_job)
                    if not self.plk_end_job_run_log:
                        if self.is_resume_run and self.prev_run_log_dir:
                            self.plk_end_job_run_log = Tools.get_log_file_by_job_name(self.prev_run_log_dir, plk_end_job)

    def _set_plk_start_2(self):
        if self.plk_start_job_run_log:
            self.plk_start_time_str, _, self.plk_start_job_errors = Tools.get_job_start_end_time_errors(self.plk_start_job_run_log, plk_start_job)
            if self.plk_start_job_errors == '1':
                self.comment = 'Job {0} had an error.'.format(plk_start_job) if self.comment is None \
                               else self.comment + ' Job {0} had an error.'.format(plk_start_job)

    def _set_plk_end_2(self):
        if self.plk_end_job_run_log:
            _, self.plk_end_time_str, self.plk_end_job_errors = Tools.get_job_start_end_time_errors(self.plk_end_job_run_log, plk_end_job)
            if self.plk_end_job_errors == '1':
                self.comment = 'Job {0} had an error.'.format(plk_end_job) if self.comment is None \
                               else self.comment + ' Job {0} had an error.'.format(plk_end_job)

    def _set_plk_runtime(self):
        if self.plk_start_time_str and self.plk_end_time_str:
            self.plk_runtime = Tools.get_runtime(self.plk_start_time_str, self.plk_end_time_str)

    def _set_plk_volume(self):
        if self.plk_start_time_str and self.plk_end_time_str:
            search_start_time = dt.strftime(dt.strptime(self.plk_start_time_str, date_time_format1) - search_time_buffer, date_time_format1)
            search_end_time = dt.strftime(dt.strptime(self.plk_end_time_str, date_time_format1) + search_time_buffer, date_time_format1)
            cmd = str('find {baklogdir} -name "{log_file_keyword}*.log" -type f -newermt "{start_dt}" ! -newermt "{end_dt}" ' +
                      '| wc -l > {tmpfile}').format(baklogdir=backup_dir + '/'+ plk_job + '*',
                                                    log_file_keyword=plk_search_keyword,
                                                    start_dt=search_start_time,
                                                    end_dt=search_end_time,
                                                    tmpfile=tmp_file)
            logger.info('EXEC: _set_plk_volume executed {0}'.format(cmd))
            os.system(cmd)
            with open(tmp_file) as f:
                volume = f.read().rstrip('\n')
            if volume > '0':
                self.plk_volume = volume
            else:
                cmd = str('find {joblogdir} -name "{log_file_keyword}*.log" -type f -newermt "{start_dt}" ! -newermt "{end_dt}" ' +
                          '| wc -l > {tmpfile}').format(joblogdir=hlm_log_dir + '/'+ plk_job + '*',
                                                        log_file_keyword=plk_search_keyword,
                                                        start_dt=search_start_time,
                                                        end_dt=search_end_time,
                                                        tmpfile=tmp_file)
                logger.info('EXEC: _set_plk_volume executed {0}'.format(cmd))
                os.system(cmd)
                with open(tmp_file) as f:
                    volume = f.read().rstrip('\n')
                if volume > '0':
                    self.plk_volume = volume

    def _set_stb_start_job_run_log(self):
        if self.run_type != 'Purge' and self.log_dir:
            self.stb_start_job_run_log = Tools.get_log_file_by_job_name(self.log_dir, stb_start_job)
            if not self.stb_start_job_run_log:
                if self.is_resumed:
                    self.stb_start_job_run_log = Tools.get_log_file_by_job_name(self.resume_run_log_dir, stb_start_job)
                    if not self.stb_start_job_run_log:
                        if self.is_resume_run and self.prev_run_log_dir:
                            self.stb_start_job_run_log = Tools.get_log_file_by_job_name(self.prev_run_log_dir, stb_start_job)

    def _set_stb_end_job_run_log(self):
        if self.run_type != 'Purge' and self.log_dir:
            self.stb_end_job_run_log = Tools.get_log_file_by_job_name(self.log_dir, stb_end_job)
            if not self.stb_end_job_run_log:
                if self.is_resumed:
                    self.stb_end_job_run_log = Tools.get_log_file_by_job_name(self.resume_run_log_dir, stb_end_job)
                    if not self.stb_end_job_run_log:
                        if self.is_resume_run and self.prev_run_log_dir:
                            self.stb_end_job_run_log = Tools.get_log_file_by_job_name(self.prev_run_log_dir, stb_end_job)

    def _set_stb_start_2(self):
        if self.stb_start_job_run_log:
            self.stb_start_time_str, _, self.stb_start_job_errors = Tools.get_job_start_end_time_errors(self.stb_start_job_run_log, stb_start_job)
            if self.stb_start_job_errors == '1':
                self.comment = 'Job {0} had an error.'.format(stb_start_job) if self.comment is None \
                               else self.comment + ' Job {0} had an error.'.format(stb_start_job)

    def _set_stb_end_2(self):
        if self.stb_end_job_run_log:
            _, self.stb_end_time_str, self.stb_end_job_errors = Tools.get_job_start_end_time_errors(self.stb_end_job_run_log, stb_end_job)
            if self.stb_end_job_errors == '1':
                self.comment = 'Job {0} had an error.'.format(stb_end_job) if self.comment is None \
                               else self.comment + ' Job {0} had an error.'.format(stb_end_job)

    def _set_stb_runtime(self):
        if self.stb_start_time_str and self.stb_end_time_str:
            self.stb_runtime = Tools.get_runtime(self.stb_start_time_str, self.stb_end_time_str)

    def _set_stb_volume(self):
        if self.stb_start_time_str and self.stb_end_time_str:
            search_start_time = dt.strftime(dt.strptime(self.stb_start_time_str, date_time_format1) - search_time_buffer, date_time_format1)
            search_end_time = dt.strftime(dt.strptime(self.stb_end_time_str, date_time_format1) + search_time_buffer, date_time_format1)
            cmd = str('find {baklogdir} -name "{log_file_keyword}*.log" -type f -newermt "{start_dt}" ! -newermt "{end_dt}" ' +
                      '| wc -l > {tmpfile}').format(baklogdir=backup_dir + '/'+ stb_job + '*',
                                                    log_file_keyword=stb_search_keyword,
                                                    start_dt=search_start_time,
                                                    end_dt=search_end_time,
                                                    tmpfile=tmp_file)
            logger.info('EXEC: _set_stb_volume executed {0}'.format(cmd))
            os.system(cmd)
            with open(tmp_file) as f:
                volume = f.read().rstrip('\n')
            if volume > '0':
                self.stb_volume = volume
            else:
                cmd = str('find {joblogdir} -name "{log_file_keyword}*.log" -type f -newermt "{start_dt}" ! -newermt "{end_dt}" ' +
                          '| wc -l > {tmpfile}').format(joblogdir=hlm_log_dir + '/'+ stb_job + '*',
                                                        log_file_keyword=stb_search_keyword,
                                                        start_dt=search_start_time,
                                                        end_dt=search_end_time,
                                                        tmpfile=tmp_file)
                logger.info('EXEC: _set_stb_volume executed {0}'.format(cmd))
                os.system(cmd)
                with open(tmp_file) as f:
                    volume = f.read().rstrip('\n')
                if volume > '0':
                    self.stb_volume = volume

    def _set_purge_3(self):
        if self.run_type == 'Purge':
            if self.log_file:
                if Tools.job_in_log(purge_job, self.log_file):
                    self.purge_start_time_str, self.purge_end_time_str, self.purge_errors = Tools.get_job_start_end_time_errors(self.log_file, purge_job)
                    if self.purge_errors == '1':
                        self.comment = 'Job {0} had an error.'.format(purge_job)
            elif self.resume_run_log_file:
                if Tools.job_in_log(purge_job, self.resume_run_log_file):
                    self.purge_start_time_str, self.purge_end_time_str, self.purge_errors = Tools.get_job_start_end_time_errors(self.resume_run_log_file, purge_job)
                    if self.purge_errors == '1':
                        self.comment = 'Job {0} had an error.'.format(purge_job)

    def _set_purge_runtime(self):
        if self.purge_start_time_str and self.purge_end_time_str:
            self.purge_runtime = Tools.get_runtime(self.purge_start_time_str, self.purge_end_time_str)

    def _set_purge_volume_2(self):
        if self.purge_start_time_str and self.purge_end_time_str:
            search_start_time = dt.strftime(dt.strptime(self.purge_start_time_str, date_time_format1) - search_time_buffer, date_time_format1)
            search_end_time = dt.strftime(dt.strptime(self.purge_end_time_str, date_time_format1) + search_time_buffer, date_time_format1)
            cmd1 = str('find {baklogdir} -name {purge_log} -type f -newermt "{start_dt}" ! -newermt "{end_dt}"'
                       ).format(baklogdir=backup_dir,
                                purge_log=purge_profile_log,
                                start_dt=search_start_time,
                                end_dt=search_end_time)
            purge_profile_log_full_path = Tools.exec_os_cmd(cmd1)
            if not purge_profile_log_full_path:
                cmd1 = str('find {joblogdir} -name {purge_log} -type f -newermt "{start_dt}" ! -newermt "{end_dt}"'
                           ).format(joblogdir=hlm_log_dir + '/' + purge_job,
                                    purge_log=purge_profile_log,
                                    start_dt=search_start_time,
                                    end_dt=search_end_time)
                purge_profile_log_full_path = Tools.exec_os_cmd(cmd1)

            cmd2 = str('find {baklogdir} -name {purge_log} -type f -newermt "{start_dt}" ! -newermt "{end_dt}"'
                       ).format(baklogdir=backup_dir,
                                purge_log=purge_custom_log,
                                start_dt=search_start_time,
                                end_dt=search_end_time)
            purge_custom_log_full_path = Tools.exec_os_cmd(cmd2)
            if not purge_custom_log_full_path:
                cmd2 = str('find {joblogdir} -name {purge_log} -type f -newermt "{start_dt}" ! -newermt "{end_dt}"'
                           ).format(joblogdir=hlm_log_dir + '/' + purge_job,
                                    purge_log=purge_custom_log,
                                    start_dt=search_start_time,
                                    end_dt=search_end_time)
                purge_custom_log_full_path = Tools.exec_os_cmd(cmd2)

            if purge_profile_log_full_path:
                cmd3 = 'grep "rows were deleted" {0}'.format(purge_profile_log_full_path)
                purge_profile_volume = sum([int(item.partition('rows')[0].rstrip().partition(':')[-1].strip().replace('No', '0'))
                                            for item in Tools.exec_os_cmd(cmd3).split('\n')])
                self.purge_volume_profile = str(purge_profile_volume)
            if purge_custom_log_full_path:
                cmd4 = 'grep "rows were deleted" {0}'.format(purge_custom_log_full_path)
                purge_custom_volume = sum([int(item.partition('rows')[0].rstrip().partition(':')[-1].strip().replace('No', '0'))
                                           for item in Tools.exec_os_cmd(cmd4).split('\n')])
                self.purge_volume_custom_data = str(purge_custom_volume)

    def _set_stb_results_4(self):
        if self.log_file:
            self.stb_results_start_time_str, self.stb_results_end_time_str, self.stb_results_errors = Tools.get_job_start_end_time_errors(self.log_file, stb_results_job)
            if self.stb_results_start_time_str and self.stb_results_end_time_str:
                start_time = dt.strptime(self.stb_results_start_time_str, date_time_format1)
                end_time = dt.strptime(self.stb_results_end_time_str, date_time_format1)
                runtime = end_time - start_time
                seconds = runtime.days * 24 * 3600 + runtime.seconds
                self.stb_results_runtime = Tools.seconds_to_timeinterval(seconds, has_seconds_part=True)
            if self.stb_results_errors == '1':
                if self.comment is None:
                    self.comment = 'Job {0} had an error.'.format(stb_results_job)
                else:
                    self.comment = self.comment + ' Job {0} had an error.'.format(stb_results_job)

    def load(self):
        self._set_sched_file_6()
        self._set_is_complete_run()
        self._set_has_errors()
        self._set_is_resume_run()
        self._set_to_be_resumed()
        self._set_resume_run_log_dir_3()
        self._set_time_etlin_finished()
        self._set_time_batch_completed()
        self._set_total_runtime()
        self._set_plk_start_job_run_log()
        self._set_plk_end_job_run_log()
        self._set_plk_start_2()
        self._set_plk_end_2()
        self._set_plk_runtime()
        self._set_plk_volume()
        self._set_stb_start_job_run_log()
        self._set_stb_end_job_run_log()
        self._set_stb_start_2()
        self._set_stb_end_2()
        self._set_stb_runtime()
        self._set_stb_volume()
        self._set_purge_3()
        self._set_purge_runtime()
        self._set_purge_volume_2()
        self._set_stb_results_4()

    def log_me(self):
        logger.info('\n@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        logger.info('run_date_str = {0}'.format(self.run_date_str))
        logger.info('sched = {0}'.format(self.sched))
        logger.info('run_type = {0}'.format(self.run_type))
        logger.info('run_weekday_rpt = {0}'.format(self.run_weekday_rpt))
        logger.info('run_date_str_rpt = {0}'.format(self.run_date_str_rpt))
        logger.info('sched_file = {0}'.format(self.sched_file))
        logger.info('log_dir = {0}'.format(self.log_dir))
        logger.info('log_file = {0}'.format(self.log_file))
        logger.info('prev_run_sched_file = {0}'.format(self.prev_run_sched_file))
        logger.info('prev_run_log_dir = {0}'.format(self.prev_run_log_dir))
        logger.info('prev_run_log_file = {0}'.format(self.prev_run_log_file))
        logger.info('is_complete_run = {0}'.format(self.is_complete_run))
        logger.info('has_errors = {0}'.format(self.has_errors))
        logger.info('is_resume_run = {0}'.format(self.is_resume_run))
        logger.info('to_be_resumed = {0}'.format(self.to_be_resumed))
        logger.info('nextday_date_str = {0}'.format(self.nextday_date_str))
        logger.info('resume_run_log_dir = {0}'.format(self.resume_run_log_dir))
        logger.info('resume_run_log_file = {0}'.format(self.resume_run_log_file))
        logger.info('is_resumed = {0}'.format(self.is_resumed))
        logger.info('time_etlin_finished = {0}'.format(self.time_etlin_finished))
        logger.info('time_batch_completed = {0}'.format(self.time_batch_completed))
        logger.info('total_runtime = {0}'.format(self.total_runtime))
        logger.info('total_runtime_minutes = {0}'.format(self.total_runtime_minutes))
        logger.info('plk_start_job_run_log = {0}'.format(self.plk_start_job_run_log))
        logger.info('plk_end_job_run_log = {0}'.format(self.plk_end_job_run_log))
        logger.info('plk_start_time_str = {0}'.format(self.plk_start_time_str))
        logger.info('plk_start_job_errors = {0}'.format(self.plk_start_job_errors))
        logger.info('plk_end_time_str = {0}'.format(self.plk_end_time_str))
        logger.info('plk_end_job_errors = {0}'.format(self.plk_end_job_errors))
        logger.info('plk_runtime = {0}'.format(self.plk_runtime))
        logger.info('plk_volume = {0}'.format(self.plk_volume))
        logger.info('stb_start_job_run_log = {0}'.format(self.stb_start_job_run_log))
        logger.info('stb_end_job_run_log = {0}'.format(self.stb_end_job_run_log))
        logger.info('stb_start_time_str = {0}'.format(self.stb_start_time_str))
        logger.info('stb_start_job_errors = {0}'.format(self.stb_start_job_errors))
        logger.info('stb_end_time_str = {0}'.format(self.stb_end_time_str))
        logger.info('stb_end_job_errors = {0}'.format(self.stb_end_job_errors))
        logger.info('stb_runtime = {0}'.format(self.stb_runtime))
        logger.info('stb_volume = {0}'.format(self.stb_volume))
        logger.info('purge_start_time_str = {0}'.format(self.purge_start_time_str))
        logger.info('purge_end_time_str = {0}'.format(self.purge_end_time_str))
        logger.info('purge_errors = {0}'.format(self.purge_errors))
        logger.info('purge_runtime = {0}'.format(self.purge_runtime))
        logger.info('purge_volume_profile = {0}'.format(self.purge_volume_profile))
        logger.info('purge_volume_custom_data = {0}'.format(self.purge_volume_custom_data))
        logger.info('stb_results_start_time_str = {0}'.format(self.stb_results_start_time_str))
        logger.info('stb_results_end_time_str = {0}'.format(self.stb_results_end_time_str))
        logger.info('stb_results_errors = {0}'.format(self.stb_results_errors))
        logger.info('stb_results_runtime = {0}'.format(self.stb_results_runtime))
        logger.info('comment = {0}'.format(self.comment))
        logger.info('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n')


if init_run:
    with open(log_file, 'w') as f:
        f.write('')
    with open(report_file, 'w') as f:
        f.write(report_file_header + '\n')
    with open(stb_results_file, 'w') as f:
        f.write(stb_results_file_header + '\n')

    run_date = start_date
    while run_date <= end_date:
        email_body = ''
        email_body_zip = []
        Tools.create_report(run_date)
        run_date = run_date + td(days=1)
else:
    run_date = end_date
    Tools.create_report(run_date)


Tools.send_mail(email_server, email_from, email_to, email_subject, email_body, files=email_attachment)
"""