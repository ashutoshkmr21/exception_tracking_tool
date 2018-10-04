import sys, os, json, mimetypes, re, smtplib, datetime, time, email
from collections import defaultdict
import email.mime.application
from google.cloud import bigquery


BQ_EXCEPTIONS_FILE = 'bq_exceptions_content.txt'
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

FILE_NAMES= ['FLIGHT', 'HOTEL', 'BUS', 'ERECHARGE', 'RAIL', 'COM', 'ALL']
MAIL_RECEPIENTS = {'FLIGHT':{'id':'idtech@via.com','sg':'sgtech@via.com','ph':'phtech@via.com','ae':'uaetech@via.com',
                             'om':'uaetech@via.com', 'sa':'uaetech@via.com', 'th':'sgtech@via.com', 'hk':'sgtech@via.com'},
                   'HOTEL':'intlhoteltechnology@via.com',
                   'BUS':'internationaltech@via.com', 'ERECHARGE':'internationaltech@via.com', 'RAIL':'internationaltech@via.com',
                    'COM':'intlpayments@via.com', 'ALL':'internationaltech@via.com'}
THRESHOLD_MAP= {'FLIGHT':50, 'HOTEL':50, 'BUS':50, 'ERECHARGE':50, 'RAIL':50, 'COM':50, 'ALL':50 }

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


def send_bq_mail(country, file, exc):
    to_mail = 'internationaltech@via.com'
    sender = 'logserver@via.com'
    receivers = [to_mail]
    msg = email.mime.Multipart.MIMEMultipart()
    msg['Subject'] = 'Error while uploading to BQ for country: ' + country +  'file : ' + file
    msg['From'] = 'logserver@via.com'
    msg['To'] = to_mail
    body = email.mime.Text.MIMEText(exc)
    msg.attach(body)
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, msg.as_string())
    except SMTPException:
        pass

def send_mail(country, file):
    if '.java' in mail_string and file != 'ALL':
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


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def load_data_from_file(dataset_name, table_name, source_file_name):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    # Reload the table to get the schema.
    table.reload()

    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job = table.upload_from_file(
            source_file, source_format='NEWLINE_DELIMITED_JSON')

    wait_for_job(job)

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))



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
            exc_data = {}
            exc_data['count'] = item[1]
            exc_data['stacktrace'] = LOG_MAPPING.get(item[0])
            exc_data['name'] = LOG_MAPPING.get(item[0]).split("\n")[0]
            exc_data['filename'] = product
            exc_data['generation_time'] = str(datetime.date.today())
            exc_data['class_name'] = LOG_MAPPING.get(item[0]).split("\n")[0].split('-')[0].strip()
            exc_data['group_mail'] = MAIL_RECEPIENTS.get(product).get(country_code) if 'FLIGHT' == product else MAIL_RECEPIENTS.get(product)
            with open(BQ_EXCEPTIONS_FILE, 'a') as bq_exception_file:
                bq_exception_file.write(json.dumps(exc_data) + '\n')
            mail_string += "\n\n" + str(item[1]) + " : " + LOG_MAPPING.get(item[0])
    print mail_string

for country in COUNTRIES:
    for file in FILE_NAMES:
        file_name = file + '.log-' + str(datetime.date.today() - datetime.timedelta(days=1))
        for root, dirname, filenames in os.walk('/logbkp/prod/' + country):
            if os.path.isfile(os.path.join(root, file_name)):
                # print os.path.join(root, file_name)
                processFile(os.path.join(root, file_name))
        print 'for country ', country, 'and file ', file, '\n\n******************************\n'
        open(BQ_EXCEPTIONS_FILE, 'w').close()
        process_mail_content(country, file)
        send_mail(country, file)
        try:
            load_data_from_file('exceptions',country + '_application_exception', BQ_EXCEPTIONS_FILE)
        except Exception as e:
            send_bq_mail(country, file, str(e))
            pass
        exceptions = defaultdict(int)
        threshold_logs = defaultdict(int)
        LOG_MAP_COUNT = defaultdict(int)
        LOG_MAPPING = {}
        time.sleep(2)

