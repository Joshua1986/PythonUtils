# coding=utf-8
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# 邮件设置
sender = ''  # 发送人
smtpserver = ''  # 邮件发送地址
receiver_list = ["mu.dong@163.com", "mu.dong@gmail.com"]


# 发邮件的主要函数
def sendmail(subject, context, receiver=receiver_list):
    msgRoot = MIMEMultipart('mixed')
    msgRoot['Subject'] = subject
    msgRoot.attach(MIMEText(context, 'plain', 'utf-8'))
    
    smtp = smtplib.SMTP()
    smtp.connect(smtpserver)
    for r in receiver:
        smtp.sendmail(sender, r, msgRoot.as_string())
    smtp.quit()

if __name__ == "__main__":
    sendmail("我只是个测试程序", "我只是个测试程序而已，你咬我啊~~~", ["mu.dong@163.com"])
