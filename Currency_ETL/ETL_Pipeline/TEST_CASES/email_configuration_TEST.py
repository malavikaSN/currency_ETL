import smtplib
from email.mime.text import MIMEText

smtp_host = 'smtp.gmail.com'
smtp_user = '@gmail.com'
smtp_password = ''
smtp_port = 587


subject = 'Test Email'
body = 'This is a test email.'
msg = MIMEText(body)
msg['Subject'] = subject
msg['From'] = smtp_user
msg['To'] = '@gmail.com'


try:
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, msg['To'], msg.as_string())
        print("Email sent successfully")
except smtplib.SMTPAuthenticationError as e:
    print(f"Authentication failed: {e}")