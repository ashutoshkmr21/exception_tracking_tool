import sys, os, json, mimetypes, re, smtplib, datetime, time, email, zipfile
from collections import defaultdict
import email.mime.application

LOG_MAPPING = {}
LOG_MAP_COUNT = defaultdict(int)
REGEX_PATTERN = 'FMN\w+'
THRESHOLD_LOGS_FILE='threshold_logs_key.txt'
THRESHOLD_LOGS_DATA = json.load(open(THRESHOLD_LOGS_FILE, 'r'))
DELIMETER='__'
DEFAULT_THRESHOLD_VAL = '50'
REGEX = re.compile("(^\tat |Exception|^Caused by: |^\t... \\d+ more)")
REGEX_DATE = re.compile('\d{4}[-]\d{2}[-]\d{2}')
# Usually, all inner lines of a stack trace will be "at" or "Caused by" lines.
# With one exception: the line following a "nested exception is" line does not
# follow that convention. Due to that, this line is handled separately.
CONT = re.compile("; nested exception is: *$")
TRIM_TIMESTAMP = re.compile("|ERROR|INFO")

FILE_NAMES = ['FLIGHT', 'HOTEL', 'BUS', 'ERECHARGE', 'RAIL', 'COM']
MAIL_RECEPIENTS = {
'FLIGHT': {'id': 'idtech@via.com', 'sg': 'sgtech@via.com', 'ph': 'phtech@via.com', 'ae': 'uaetech@via.com',
           'om': 'uaetech@via.com', 'sa': 'uaetech@via.com', 'th': 'sgtech@via.com', 'hk': 'sgtech@via.com'},
'HOTEL': 'intlhoteltechnology@via.com',
'BUS': 'internationaltech@via.com', 'ERECHARGE': 'internationaltech@via.com', 'RAIL': 'internationaltech@via.com',
'COM': 'intlpayments@via.com'}

MAIL_COUNTRY = {'id': 'idtech@via.com', 'ph': 'phtech@via.com', 'sg': 'sgtech@via.com', 'ae': 'uaetech@via.com',
                'om': 'uaetech@via.com', 'sa': 'uaetech@via.com', 'th': 'sgtech@via.com', 'hk': 'sgtech@via.com'}
THRESHOLD_MAP = {'FLIGHT': 50, 'HOTEL': 50, 'BUS': 50, 'ERECHARGE': 50, 'RAIL': 50, 'COM': 50}

COUNTRIES = ['id', 'ph', 'sg', 'th', 'hk', 'ae', 'om', 'sa']
exceptions = defaultdict(int)
threshold_logs = defaultdict(int)
mail_string = ''

def populate_log_map(log_stack, count):
    key = "\n".join(log_stack.split('\n', 2)[:2]).strip()
    LOG_MAP_COUNT[key] +=count
    if not LOG_MAPPING.get(key):
        LOG_MAPPING[key] = log_stack

def register_threshold_logs(logs, count):
    threshold_logs[logs] += count

def registerException(exc):
    exceptions[re.sub(REGEX_PATTERN, '', exc)] += 1

def processFile(fileName):
  with open(fileName, "r") as fh:
    currentMatch = None
    lastLine = None
    addNextLine = False
    for line in fh.readlines():
      if addNextLine and currentMatch != None:
        addNextLine = False
        currentMatch += line
        continue
      match = REGEX.search(line) != None
      date_match = REGEX_DATE.search(line) != None
      if match and currentMatch != None and not date_match:
        currentMatch += line
      elif match:
        if lastLine != None and not date_match:
          currentMatch = lastLine + line
        else:
          currentMatch = line
          if len(re.split('ERROR ', currentMatch, 1)) == 2:
            currentMatch = '####$$$$' + re.split('ERROR ', currentMatch, 1)[1]
            if lastLine != None and '####$$$$' in lastLine:
              registerException(lastLine.split('####$$$$')[1])
              lastLine = None
      elif currentMatch==None:
        if lastLine != None and not date_match:
          currentMatch = lastLine + line
      else:
        if currentMatch != None and date_match:
          registerException(currentMatch.split('####$$$$')[1] if '####$$$$' in currentMatch  else currentMatch)
          lastLine = None
          currentMatch = None
        elif currentMatch != None:
          currentMatch += line
          continue
      if len(re.split('ERROR ', line, 1)) == 2:
        if lastLine != None and '####$$$$' in lastLine:
          registerException(lastLine.split('####$$$$')[1])
          currentMatch = None
        line = '####$$$$' + re.split('ERROR ', line, 1)[1]
      lastLine = line
      addNextLine = CONT.search(line) != None
    # If last line in file was a stack trace
    if currentMatch != None:
      registerException(currentMatch.split('####$$$$')[1] if '####$$$$' in currentMatch  else currentMatch)

def send_mail(country, file):
    if '.java' in mail_string:
        if 'FLIGHT' == file:
            to_mail = MAIL_RECEPIENTS.get(file).get(country)
        else:
            to_mail = MAIL_RECEPIENTS.get(file)
        print 'to email to send: ' + to_mail
        file_to_attach = file + '.txt'
        with open(file_to_attach, 'w') as save_attachment:
            save_attachment.write(mail_string)
        sender = 'logserver@via.com'
        receivers = [to_mail]
        # Create a text/plain message
        msg = email.mime.Multipart.MIMEMultipart()
        msg['Subject'] = 'Log Exceptions for %s for country %s' %(file, country)
        msg['From'] = 'logserver@via.com'
        msg['To'] = to_mail
        # The main body is just another attachment
        body = email.mime.Text.MIMEText("""Hi,
        Please find the attached log exceptions.""")
        msg.attach(body)
        # txt attachment
        filename=file_to_attach
        fp=open(filename,'rb')
        att = email.mime.application.MIMEApplication(fp.read(),_subtype="txt")
        fp.close()
        att.add_header('Content-Disposition','attachment',filename=filename)
        msg.attach(att)
        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, msg.as_string())
        except SMTPException:
            pass


def send_mail(country):
    sender = 'logserver@via.com'
    recipients = [MAIL_COUNTRY.get(country)]
    # Create a text/plain message
    msg = email.mime.Multipart.MIMEMultipart()
    msg['Subject'] = 'Log Exceptions for country %s' %(country)
    msg['From'] = 'logserver@via.com'
    msg['To'] = ','.join(recipients)

    # The main body is just another attachment
    body = email.mime.Text.MIMEText("""Hi,
        Please find the attached daily Log Exceptions Reports.""")
    msg.attach(body)

    # Zip attachment
    filename = 'log_report.zip'
    fp = open(filename, 'rb')
    att = email.mime.application.MIMEApplication(fp.read())
    fp.close()
    att.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(att)
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, recipients, msg.as_string())
    except smtplib.SMTPException as e:
        # print 'Exception occured while sending mail' + e
        pass


def process_mail_content(country_code, product):
    global mail_string
    mail_string = ''
    for item in sorted(exceptions.items(), key=lambda e: e[1], reverse=True):
        if item[0].count('\n') > 3:
            populate_log_map(item[0], item[1])
    for item in sorted(LOG_MAP_COUNT.items(), key=lambda  e: e[1], reverse=True):
        thres_val = THRESHOLD_LOGS_DATA.get(country_code+DELIMETER+item[0].split('\n')[0].strip()) if THRESHOLD_LOGS_DATA.get(country_code+DELIMETER+item[0].split('\n')[0].strip()) \
            else THRESHOLD_LOGS_DATA.get('ALL'+DELIMETER+item[0].split('\n')[0].strip()) if THRESHOLD_LOGS_DATA.get('ALL'+DELIMETER+item[0].split('\n')[0].strip()) \
            else THRESHOLD_MAP.get(product)
        if item[1] > int(thres_val):
            mail_string += "\n\n" + str(item[1]) + " : " + LOG_MAPPING.get(item[0])
    print mail_string

def populate_file(filename):
    f = open(filename, 'w')
    f.write(mail_string)
    f.close()


def make_zipfile(output_filename, source_dir):
    relroot = os.path.abspath(os.path.join(source_dir, os.pardir))
    with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zip:
        for root, dirs, files in os.walk(source_dir):
            # add directory (needed for empty dirs)
            zip.write(root, os.path.relpath(root, relroot))
            for file in files:
                filename = os.path.join(root, file)
                if os.path.isfile(filename): # regular files only
                    arcname = os.path.join(os.path.relpath(root, relroot), file)
                    zip.write(filename, arcname)

def delete_files():
    os.remove('log_report.zip')
    filelist = [ f for f in os.listdir("log_report/")]
    for f in filelist:
        os.remove("log_report/" + f)

delete_files()

for country in COUNTRIES:
    for file in FILE_NAMES:
        file_name = file + '.log-' + str(datetime.date.today() - datetime.timedelta(days=1))
        for root, dirname, filenames in os.walk('/logbkp/prod/' + country):
            if os.path.isfile(os.path.join(root, file_name)):
                # print os.path.join(root, file_name)
                processFile(os.path.join(root, file_name))
        print 'for country ', country, 'and file ', file, '\n\n******************************\n'
        process_mail_content(country, file)
        populate_file('log_report/'+file+'.txt')
        # send_mail(country, file)
        exceptions = defaultdict(int)
        threshold_logs = defaultdict(int)
        LOG_MAP_COUNT = defaultdict(int)
        LOG_MAPPING = {}
        time.sleep(2)
    make_zipfile("log_report.zip", "log_report")
    send_mail(country)
    delete_files()

