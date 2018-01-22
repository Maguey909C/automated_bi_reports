
msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = ", ".join(emailto)
msg["Subject"] = "This is the subject"
msg.preamble = "NA"
body = """
Hey, This is the body of the email.
"""

body = MIMEText(body) # convert the body to a MIME compatible string
msg.attach(body)

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
print ("Email Sent")
