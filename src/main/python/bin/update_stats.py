import alblogs
import logging
import datetime

LOGGER = None


def read_arguments():
    argparser = alblogs.get_default_argparser()
    argparser.add_argument("date", metavar="DATE", type=str, help="Date for which to process ALB logs (yyyy-mm-dd)")

    args = argparser.parse_args()

    # try to convert args.date to check if it is a valid date
    datetime.datetime.strptime(args.date, '%Y-%m-%d')

    return args


def load_url_stats(logsdb, statsdb):
    rows = logsdb.query_url_stats()

    for row in rows:
        statsdb.save_url_stats(row)

    statsdb.commit()


def load_target_address_stats(logsdb, statsdb):
    rows = logsdb.query_target_address_stats()

    for row in rows:
        statsdb.save_target_address_stats(row)

    statsdb.commit()


# ------------------------------ MAIN PROGRAM ------------------------------


args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Starting update_stats.py")

config = alblogs.get_configuration()

logsdb_file = "{}/{}-alblogs.db".format(config.get_data_dir(), args.date)
statsdb_file = "{}/stats.db".format(config.get_data_dir(), args.date)

logsdb = alblogs.open_logsdb(logsdb_file, create=False)
statsdb = alblogs.open_statsdb(statsdb_file, create=True)

load_url_stats(logsdb, statsdb)
load_target_address_stats(logsdb, statsdb)
