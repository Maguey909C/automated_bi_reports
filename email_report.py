import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

emailfrom = "originaluser@company.com"
emailto = ["email1@stuff.com","email2@stuff.com"]
fileToSend = ["/path/to/file/name_of_file1.csv","/path/to/file/name_of_file2.csv","/path/to/file/name_of_file3.csv"]

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = ", ".join(emailto)
msg["Subject"] = "This is the subject header"
msg.preamble = "NA"
body = 'Hey, this is where the body of the email is written.'

for specific_file in fileToSend:
    ctype, encoding = mimetypes.guess_type(specific_file)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(specific_file)
        # Note: we should handle calculating the charset
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "image":
        fp = open(specific_file, "rb")
        attachment = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(specific_file, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=os.path.basename(specific_file))
    msg.attach(attachment)

server = smtplib.SMTP('localhost', portnumber)
server.sendmail(emailfrom, emailto, msg.as_string())
server.quit()
print ("Sent Email")
