"""
Utilities I developed.
"""

def print_mro(cls):
    print(', '.join(c.__name__ for c in cls.__mro__))



import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_mail(mail_server, send_from, send_to_list, subject, text, text_color='black', files=None, username=None, password=None):
    assert send_to_list
    assert isinstance(send_to_list, list)

    msg = MIMEMultipart('related')
    msg['From'] = send_from
    msg['To'] = ','.join(send_to_list)
    msg['Subject'] = subject
    warnings = []
    attachments = []

    if files and isinstance(files, list):
        for file in files:
            try:
                with open(file, 'rb') as f:
                    attachment = MIMEApplication(f.read())
                # After the file is closed
                attachment['Content-Disposition'] = 'attachment; filename={}'.format(basename(file))
                attachments.append(attachment)
            except FileNotFoundError:
                warnings.append('WARNING: Attachment {} was not found!'.format(basename(file)))

    body_template = '<pre><font color={}>{}</font></pre>'
    if warnings:
        body_template = '<pre><font color={}>{}</font></pre>' + '<br><br>' + '<pre>{}</pre>'

    msg.attach(MIMEText(body_template.format(text_color, text, '\n'.join(warnings)), 'html', 'utf-8'))

    if attachments:
        for attachment in attachments:
            msg.attach(attachment)

    smtp = smtplib.SMTP()
    smtp.connect(mail_server)
    if username is not None and password is not None:
        smtp.login(username, password)
    smtp.sendmail(send_from, send_to_list, msg.as_string())
    smtp.close()

    return True
