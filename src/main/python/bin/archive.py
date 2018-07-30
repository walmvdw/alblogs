import alblogs
import logging
import datetime
import os.path
import gzip

LOGGER = None

# 10 MB buffer for reading/writing
BUFFER_SIZE = 10 * 1024 * 1024


def read_arguments():
    argparser = alblogs.get_default_argparser()
    argparser.add_argument("date", metavar="DATE", type=str, help="Date for which to process ALB logs (yyyy-mm-dd)")
    argparser.add_argument("-a", "--age", action='store', default=7, metavar="AGE", type=int, help="Minimum age (in days) for files to archive")

    args = argparser.parse_args()

    # try to convert args.date to check if it is a valid date
    datetime.datetime.strptime(args.date, '%Y-%m-%d')

    return args


def archive_file(filename, filepath, size, arch_dir):
    target_zip = "{}.gz".format(os.path.join(arch_dir, filename))
    if os.path.exists(target_zip):
        raise RuntimeError("Target for archiving '{}' already exists".format(target_zip))

    LOGGER.info("Start archiving {} to {}".format(filepath, target_zip))

    LOGGER.debug("Processing {:,d} bytes".format(size))

    with open(filepath, 'rb') as infile:
        with gzip.open(target_zip, 'wb') as outfile:
            buffer = infile.read(BUFFER_SIZE)
            processed = 0
            while buffer:
                outfile.write(buffer)
                processed += len(buffer)

                if processed % (5 * BUFFER_SIZE) == 0:
                    LOGGER.debug("Read {:,d} bytes".format(processed))

                buffer = infile.read(BUFFER_SIZE)

    LOGGER.debug("Read {:,d} bytes".format(processed))
    LOGGER.info("Finished archiving {} to {}".format(filepath, target_zip))
    os.remove(filepath)

# ------------------------------ MAIN PROGRAM ------------------------------

args = read_arguments()

alblogs.initialize(args)

LOGGER = logging.getLogger(__name__)
LOGGER.info("Starting archive.py")

config = alblogs.get_configuration()

arch_dir = config.get_archive_dir()
if os.path.exists(arch_dir):
    if not os.path.isdir(arch_dir):
        raise RuntimeError("Path '{}' is not a directory".format(arch_dir))
else:
    os.makedirs(arch_dir)

LOGGER.info("Start date  : {}".format(args.date))
LOGGER.info("Age         : {}".format(args.age))

age_delta = datetime.timedelta(days=args.age)

arch_date = (datetime.datetime.strptime(args.date, '%Y-%m-%d') - age_delta).strftime('%Y-%m-%d')

LOGGER.info("Archive date: {}".format(arch_date))

data_dir = config.get_data_dir()

with os.scandir(data_dir) as it:
    for entry in it:
        if entry.is_file() and entry.name.endswith("-alblogs.db") and entry.name[:10] < arch_date:
            LOGGER.info("Found file to archive: {}".format(entry.path))
            archive_file(entry.name, entry.path, entry.stat().st_size, arch_dir)


