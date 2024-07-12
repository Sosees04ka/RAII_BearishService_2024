from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from aiosmtplib import SMTP
import asyncio

from pdf import formPdf, dataLoader

EMAIL = 'orderbuyerzxc@gmail.com'
PWD = 'bhau wuvy rltg psgv'
path_to_pdf = r'output.pdf'


async def send_mail(subject, to, msg, flatId):
    await dataLoader(flatId)
    await asyncio.to_thread(formPdf, flatId)
    message = MIMEMultipart()
    message["From"] = EMAIL
    message["To"] = to
    message["Subject"] = subject
    message.attach(MIMEText(f"<html><body>{msg}</body></html>", "html", "utf-8"))
    with open(path_to_pdf, "rb") as f:
        # attach = email.mime.application.MIMEApplication(f.read(),_subtype="pdf")
        attach = MIMEApplication(f.read(), _subtype="pdf")
    attach.add_header('Content-Disposition', 'attachment', filename=str(path_to_pdf))
    message.attach(attach)
    smtp_client = SMTP(hostname="smtp.gmail.com", port=465, use_tls=True)
    async with smtp_client:
        await smtp_client.login(EMAIL, PWD)
        await smtp_client.send_message(message)
