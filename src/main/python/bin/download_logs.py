import alblogs
import logging
import os.path
import datetime
import boto3

LOGGER = None


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

config = alblogs.get_configuration()

LOGGER.info("Starting download_logs.py")
LOGGER.info("S3 bucket: {}".format(config.aws.get_bucket_name()))
LOGGER.info("base dir : {}".format(config.aws.get_base_dir()))

srclogs_dir = os.path.join(config.get_srclogs_dir(), args.date)
if os.path.exists(srclogs_dir):
    if not os.path.isdir(srclogs_dir):
        raise RuntimeError("Path '{}' is not a directory".format(srclogs_dir))
else:
    os.makedirs(srclogs_dir)

s3 = boto3.resource('s3')
bucket = s3.Bucket(config.aws.get_bucket_name())

aws_base_dir = config.aws.get_base_dir()
if not aws_base_dir.endswith("/"):
    aws_base_dir += "/"

filter_prefix = "{}{}".format(aws_base_dir, args.date.replace("-", "/"))
LOGGER.debug("Filter prefix: {}".format(filter_prefix))

for log_object in bucket.objects.filter(Prefix=filter_prefix):
    log_name = log_object.key[len(filter_prefix)+1:]
    target_file = os.path.join(srclogs_dir, log_name)
    if os.path.exists(target_file):
        raise RuntimeError("File to download '{}' already exists".format(target_file))

    LOGGER.debug("Downloading {}".format(target_file))
    log_object.Object().download_file(target_file)