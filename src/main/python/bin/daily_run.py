import alblogs
import logging
import datetime
import subprocess


def read_arguments():
    argparser = alblogs.get_default_argparser()
    argparser.add_argument("--date", "-d", metavar="DATE", type=str, default=None, help="Date for which to process ALB logs (yyyy-mm-dd)")
    argparser.add_argument("--skipdownload", "-s", action="store_true", help="When this option is provided the downloading of logs is skipped")
    argparser.add_argument("--trendfromdate", "-t", action="store", default=None, help="Start date for trend report")
    argparser.add_argument("--force", "-f", action="store_true", help="Force execution even if calculated run day is today")

    args = argparser.parse_args()

    # try to convert args.date to check if it is a valid date
    if args.date:
        datetime.datetime.strptime(args.date, '%Y-%m-%d')

    return args


def find_next_date(statsdb):
    max_date = statsdb.query_max_date()
    next_date = datetime.datetime.strptime(max_date, "%Y-%m-%d") + datetime.timedelta(days=1)

    return next_date.strftime("%Y-%m-%d")


def execute(params):
    LOGGER.debug("Execute: {}".format(params))

    result = subprocess.run(params)
    if result.returncode == 0:
        LOGGER.info("Process '{}' completed without errors".format(params[1]))
    else:
        LOGGER.error("Processes '{}' returned error, aborting execution".format(params[1]))
        raise RuntimeError("Process '{}' returned error, aborting execution".format(params[1]))


def execute_create_report(args):
    params = []
    params.append("python")
    params.append("create_report.py")
    if args.config:
        params.append("--config={}".format(args.config))

    params.append(run_date)
    execute(params)


def execute_parse_logs(args):
    params = []
    params.append("python")
    params.append("parse_logs.py")
    if args.config:
        params.append("--config={}".format(args.config))

    params.append(run_date)
    execute(params)


def execute_update_stats(args):
    params = []
    params.append("python")
    params.append("update_stats.py")
    if args.config:
        params.append("--config={}".format(args.config))

    params.append(run_date)
    execute(params)


def execute_download_logs(args):
    params = []
    params.append("python")
    params.append("download_logs.py")
    if args.config:
        params.append("--config={}".format(args.config))

    params.append(run_date)
    execute(params)


def execute_create_trend_report(args):
    params = []
    params.append("python")
    params.append("create_trend_report.py")
    if args.config:
        params.append("--config={}".format(args.config))

    if args.trendfromdate:
        params.append("--fromdate={}".format(args.trendfromdate))

    execute(params)


def execute_archive(args):
    params = []
    params.append("python")
    params.append("archive.py")
    if args.config:
        params.append("--config={}".format(args.config))

    params.append(run_date)
    execute(params)


# ------------------------------ MAIN PROGRAM ------------------------------


args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)

config = alblogs.get_configuration()
statsdb_file = "{}/stats.db".format(config.get_data_dir(), args.date)
statsdb = alblogs.open_statsdb(statsdb_file, create=False)

LOGGER.info("Starting download_logs.py")
if args.date is None:
    run_date = find_next_date(statsdb)
    LOGGER.info("No date provided, next date selected from database: {}".format(run_date))
else:
    run_date = args.date
    LOGGER.info("Date provided in commandline options: {}".format(run_date))

today = datetime.datetime.now().strftime("%Y-%m-%d")
if today == run_date and not args.force:
    raise RuntimeError("Run date is today and --force flag not specified")

if args.skipdownload:
    LOGGER.info("Skipping call to download.py because skipdownload is enabled")
else:
    execute_download_logs(args)

execute_parse_logs(args)

execute_update_stats(args)

execute_create_report(args)

execute_create_trend_report(args)

execute_archive(args)