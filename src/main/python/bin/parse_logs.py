import alblogs
import logging
import os.path
import datetime
import re
import gzip

LOGGER = None

LOG_ENTRY_RE = re.compile("(.*?) (.*?) (.*?) (.*?)\:(.*?) (.*?)\:(.*?) (.*?) (.*?) (.*?) (.*?) (.*?) (.*?) (.*?) \"(.*?) (.*?) (.*?)\" \"(.*?)\" (.*?) (.*?) (.*?) \"(.*?)\" \"(.*?)\" \"(.*?)\" (.*?) (.*?) \"(.*?)\"")

# 2018-07-24T13:25:00.488400Z
DATE_RE = re.compile("(.*?)T(\d\d)\:(\d\d)\:(\d\d)\.(\d+)Z")


def find_logs(current_path, logfiles):
    with os.scandir(current_path) as it:
        for entry in it:
            log_path = os.path.join(current_path, entry.name)
            if entry.is_file():
                logfiles.append(log_path)
                LOGGER.debug("Found log file '{}'".format(log_path))
            elif entry.is_dir():
                find_logs(log_path, logfiles)


def fix_negative(value):
    if float(value) < 0:
        return 0

    return float(value)


def parse_request(request):
    url = None
    query = None

    parts = request.split("?")
    url = parts[0]
    if len(parts) > 1:
        query = parts[1]

    return (url, query)


def parse_date(datestr):
    datepart = None
    hourpart = None
    minutepart = None
    secondspart = None
    microsecondspart = None

    match = DATE_RE.match(datestr)
    if match:
        datepart = match.group(1).strip()
        hourpart = match.group(2).strip()
        minutepart = match.group(3).strip()
        secondspart = match.group(4).strip()
        microsecondspart = match.group(5).strip()

    else:
        raise RuntimeError("DATE NOMATCH: {0}".format(datestr))

    return datepart, hourpart, minutepart, secondspart, microsecondspart


def parse_logfile(db, logname):
    LOGGER.info("Processing: '{}'".format(logname))

    log_source_id = db.add_source(logname)

    if logname.endswith(".gz"):
        LOGGER.debug("Log file is a gzip file")
        fhandle = gzip.open(logname, "rt", encoding="latin-1")
    else:
        LOGGER.debug("Log file is a normal text file file")
        fhandle = open(logname, "r", encoding="latin-1")

    count = 0
    for line in fhandle:
        count += 1
        if count % 1000 == 0:
            LOGGER.debug("Processed {} lines".format(count))

        line = line.rstrip()
        match = LOG_ENTRY_RE.match(line)
        if match:
            (request_url, request_query) = parse_request(match.group(16).strip())
            (datepart, hourpart, minutepart, secondspart, microsecondspart) = parse_date(match.group(2).strip())

            request_processing_time = fix_negative(match.group(8).strip())
            target_processing_time = fix_negative(match.group(9).strip())
            response_processing_time = fix_negative(match.group(10).strip())

            record = {"type": match.group(1).strip()
                , "timestamp": match.group(2).strip()
                , "datepart": datepart
                , "hourpart": hourpart
                , "minutepart": minutepart
                , "secondspart": secondspart
                , "microsecondspart": microsecondspart
                , "elb": match.group(3).strip()
                , "client_ip": match.group(4).strip()
                , "client_port": match.group(5).strip()
                , "target_ip": match.group(6).strip()
                , "target_port": match.group(7).strip()
                , "request_processing_time": request_processing_time
                , "target_processing_time": target_processing_time
                , "response_processing_time": response_processing_time
                , "elb_status_code": match.group(11).strip()
                , "target_status_code": match.group(12).strip()
                , "received_bytes": match.group(13).strip()
                , "sent_bytes": match.group(14).strip()
                , "request_method": match.group(15).strip()
                , "request_url": request_url
                , "request_query": request_query
                , "http_version": match.group(17).strip()
                , "user_agent": match.group(18).strip()
                , "ssl_cipher": match.group(19).strip()
                , "ssl_protocol": match.group(20).strip()
                , "target_group_arn": match.group(21).strip()
                , "trace_id": match.group(22).strip()
                , "domain_name": match.group(23).strip()
                , "chosen_cert_arn": match.group(24).strip()
                , "matched_rule_priority": match.group(25).strip()
                , "request_creation_time": match.group(26).strip()
                , "actions_executed": match.group(27).strip()
                      }
            db.save_record(log_source_id, record)

            # for key in record.keys():
            #    print("{} = {}".format(key, record[key]))
            # exit()
        else:
            LOGGER.warning("NOMATCH: {}".format(line))

    LOGGER.info("Processed {} lines".format(count))
    db.commit()


def read_arguments():
    argparser = alblogs.get_default_argparser()
    argparser.add_argument("date", metavar="DATE", type=str, help="Date for which to process ALB logs (yyyy-mm-dd)")

    args = argparser.parse_args()

    # try to convert args.date to check if it is a valid date
    datetime.datetime.strptime(args.date, '%Y-%m-%d')

    return args

# ------------------------------ MAIN PROGRAM ------------------------------


args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Starting parse_logs.py")

config = alblogs.get_configuration()

source_dir = "{}/{}".format(config.get_srclogs_dir(), args.date)
if not os.path.exists(source_dir):
    raise RuntimeError("ALB log source directory '{}' does not exist".format(source_dir))

dbfile = "{}/{}-alblogs.db".format(config.get_data_dir(), args.date)

if os.path.exists(dbfile):
    raise RuntimeError("Database '{}' already exists".format(dbfile))

db = alblogs.open_logsdb(dbfile, create=True)

logfiles = []
find_logs(source_dir, logfiles)

for logfile in logfiles:
    parse_logfile(db, logfile)
