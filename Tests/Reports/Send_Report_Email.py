import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

def send_email_report(report_file, subject, sender_email, receiver_email, smtp_server, smtp_port, smtp_username, smtp_password):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email

    # Attach the HTML report
    with open(report_file, "rb") as f:
        report_data = f.read()
    report_attachment = MIMEApplication(report_data, _subtype="html")
    report_attachment.add_header("Content-Disposition", "attachment", filename=os.path.basename(report_file))
    msg.attach(report_attachment)

    # Send the email
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

# Set your email configuration and report file
report_file = "ReportTest.html"
subject = "Pytest Report"
receiver_email = 'adtq15@gmail.com'
sender_email = 'davidtorres.estudiar@gmail.com'
smtp_server = 'smtp.gmail.com'
smtp_port = 465
smtp_user = 'davidtorres.estudiar@gmail.com'
smtp_password = 'xsedurigirplyciu'

# Send the email with the report attached
send_email_report(report_file, subject, sender_email, receiver_email, smtp_server, smtp_port, smtp_user, smtp_password)