import os.path
import sqlite3
import logging

LOGGER = None

sql = ""
sql += "INSERT INTO `stats_url`  "
sql += "(                         `log_date_id` "
sql += ",                         `log_hour_id` "
sql += ",                         `log_url_id` "
sql += ",                         `log_reqtype_id` "
sql += ",                         `elb_status_code` "
sql += ",                         `request_count` "
sql += ",                         `sum_request_processing_time_sec` "
sql += ",                         `min_request_processing_time_sec` "
sql += ",                         `max_request_processing_time_sec` "
sql += ",                         `sum_target_processing_time_sec` "
sql += ",                         `min_target_processing_time_sec` "
sql += ",                         `max_target_processing_time_sec` "
sql += ",                         `sum_response_processing_time_sec` "
sql += ",                         `min_response_processing_time_sec` "
sql += ",                         `max_response_processing_time_sec` "
sql += ",                         `sum_received_bytes` "
sql += ",                         `min_received_bytes` "
sql += ",                         `max_received_bytes` "
sql += ",                         `sum_sent_bytes` "
sql += ",                         `min_sent_bytes` "
sql += ",                         `max_sent_bytes` "
sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

STATS_URL_INSERT_SQL = sql

sql = ""
sql += "INSERT INTO `stats_target_address`  "
sql += "(                         `log_date_id` "
sql += ",                         `log_hour_id` "
sql += ",                         `log_target_address_id` "
sql += ",                         `target_port` "
sql += ",                         `request_count` "
sql += ",                         `sum_request_processing_time_sec` "
sql += ",                         `min_request_processing_time_sec` "
sql += ",                         `max_request_processing_time_sec` "
sql += ",                         `sum_target_processing_time_sec` "
sql += ",                         `min_target_processing_time_sec` "
sql += ",                         `max_target_processing_time_sec` "
sql += ",                         `sum_response_processing_time_sec` "
sql += ",                         `min_response_processing_time_sec` "
sql += ",                         `max_response_processing_time_sec` "
sql += ",                         `sum_received_bytes` "
sql += ",                         `min_received_bytes` "
sql += ",                         `max_received_bytes` "
sql += ",                         `sum_sent_bytes` "
sql += ",                         `min_sent_bytes` "
sql += ",                         `max_sent_bytes` "
sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

STATS_TARGET_ADDRESS_INSERT_SQL = sql

sql = ""
sql += "INSERT INTO `stats_status_code`  "
sql += "(                         `log_date_id` "
sql += ",                         `log_hour_id` "
sql += ",                         `target_status_code` "
sql += ",                         `elb_status_code` "
sql += ",                         `request_count` "
sql += ",                         `sum_request_processing_time_sec` "
sql += ",                         `min_request_processing_time_sec` "
sql += ",                         `max_request_processing_time_sec` "
sql += ",                         `sum_target_processing_time_sec` "
sql += ",                         `min_target_processing_time_sec` "
sql += ",                         `max_target_processing_time_sec` "
sql += ",                         `sum_response_processing_time_sec` "
sql += ",                         `min_response_processing_time_sec` "
sql += ",                         `max_response_processing_time_sec` "
sql += ",                         `sum_received_bytes` "
sql += ",                         `min_received_bytes` "
sql += ",                         `max_received_bytes` "
sql += ",                         `sum_sent_bytes` "
sql += ",                         `min_sent_bytes` "
sql += ",                         `max_sent_bytes` "
sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

STATS_STATUS_CODE_INSERT_SQL = sql


def get_log():
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)
    return LOGGER


class Database(object):
    def __init__(self, filename, create=False):
        self._filename = filename
        self._create = create
        self._conn = None
        self._curs = None

    def open(self):
        if os.path.exists(self._filename):
            get_log().info("Opening existing database {}".format(self._filename))
            return self._open_db()

        if self._create:
            get_log().info("Creating new database {}".format(self._filename))
            return self._create_db()

        raise RuntimeError("Database at path '{}' does not exist and auto create is disabled".format(self._filename))

    def _get_conn(self):
        return self._conn

    def _get_cursor(self):
        if self._curs is None:
            self._curs = self._get_conn().cursor()

        return self._curs

    def commit(self):
        if self._curs is None:
            get_log().error("No active transactions")
            raise RuntimeError("No active transaction")

        get_log().info("Committing transaction")
        self._get_conn().commit()
        self._curs is None

    def _open_db(self):
        self._conn = sqlite3.connect(self._filename)
        self._conn.row_factory = sqlite3.Row

    def _create_db(self):
        self._open_db()

        curs = self._get_cursor()

        sql = ""
        sql += "CREATE TABLE `log_date` (`id` INTEGER PRIMARY KEY "
        sql += ",                          `date` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_date_date` ON `log_date`(`date`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_hour` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `hour` INTEGER);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_hour_hour` ON `log_hour`(`hour`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_elb` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `elb` INTEGER);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_elb_elb` ON `log_elb`(`elb`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_method` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `method` INTEGER);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_method_method` ON `log_method`(`method`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_url` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `url` INTEGER);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_url_url` ON `log_url`(`url`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_http_version` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `http_version` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_http_version_http_version` ON `log_http_version`(`http_version`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_user_agent` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `user_agent` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_user_agent_user_agent` ON `log_user_agent`(`user_agent`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_target_group` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `target_group` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_target_group_target_group` ON `log_target_group`(`target_group`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_domain` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `domain` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_domain_domain` ON `log_domain`(`domain`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_reqtype` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `reqtype` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_reqtype_reqtype` ON `log_reqtype`(`reqtype`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_target_address` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `target_address` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_target_address_target_address` ON `log_target_address`(`target_address`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `log_client_address` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `client_address` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_client_address_client_address` ON `log_client_address`(`client_address`);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `stats_url` (`id` INTEGER PRIMARY KEY "
        sql += ",                         `log_date_id` INTEGER "
        sql += ",                         `log_hour_id` INTEGER "
        sql += ",                         `log_url_id` INTEGER "
        sql += ",                         `log_reqtype_id` INTEGER "
        sql += ",                         `elb_status_code` INTEGER "
        sql += ",                         `request_count` INTEGER "
        sql += ",                         `sum_request_processing_time_sec` REAL "
        sql += ",                         `min_request_processing_time_sec` REAL "
        sql += ",                         `max_request_processing_time_sec` REAL "
        sql += ",                         `sum_target_processing_time_sec` REAL "
        sql += ",                         `min_target_processing_time_sec` REAL "
        sql += ",                         `max_target_processing_time_sec` REAL "
        sql += ",                         `sum_response_processing_time_sec` REAL "
        sql += ",                         `min_response_processing_time_sec` REAL "
        sql += ",                         `max_response_processing_time_sec` REAL "
        sql += ",                         `sum_received_bytes` INTEGER "
        sql += ",                         `min_received_bytes` INTEGER "
        sql += ",                         `max_received_bytes` INTEGER "
        sql += ",                         `sum_sent_bytes` INTEGER "
        sql += ",                         `min_sent_bytes` INTEGER "
        sql += ",                         `max_sent_bytes` INTEGER "
        sql += ");"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `stats_target_address` (`id` INTEGER PRIMARY KEY "
        sql += ",                         `log_date_id` INTEGER "
        sql += ",                         `log_hour_id` INTEGER "
        sql += ",                         `log_target_address_id` INTEGER "
        sql += ",                         `target_port` INTEGER "
        sql += ",                         `request_count` INTEGER "
        sql += ",                         `sum_request_processing_time_sec` REAL "
        sql += ",                         `min_request_processing_time_sec` REAL "
        sql += ",                         `max_request_processing_time_sec` REAL "
        sql += ",                         `sum_target_processing_time_sec` REAL "
        sql += ",                         `min_target_processing_time_sec` REAL "
        sql += ",                         `max_target_processing_time_sec` REAL "
        sql += ",                         `sum_response_processing_time_sec` REAL "
        sql += ",                         `min_response_processing_time_sec` REAL "
        sql += ",                         `max_response_processing_time_sec` REAL "
        sql += ",                         `sum_received_bytes` INTEGER "
        sql += ",                         `min_received_bytes` INTEGER "
        sql += ",                         `max_received_bytes` INTEGER "
        sql += ",                         `sum_sent_bytes` INTEGER "
        sql += ",                         `min_sent_bytes` INTEGER "
        sql += ",                         `max_sent_bytes` INTEGER "
        sql += ");"
        curs.execute(sql)

        sql = ""
        sql += "CREATE TABLE `stats_status_code` (`id` INTEGER PRIMARY KEY "
        sql += ",                         `log_date_id` INTEGER "
        sql += ",                         `log_hour_id` INTEGER "
        sql += ",                         `target_status_code` INTEGER "
        sql += ",                         `elb_status_code` INTEGER "
        sql += ",                         `request_count` INTEGER "
        sql += ",                         `sum_request_processing_time_sec` REAL "
        sql += ",                         `min_request_processing_time_sec` REAL "
        sql += ",                         `max_request_processing_time_sec` REAL "
        sql += ",                         `sum_target_processing_time_sec` REAL "
        sql += ",                         `min_target_processing_time_sec` REAL "
        sql += ",                         `max_target_processing_time_sec` REAL "
        sql += ",                         `sum_response_processing_time_sec` REAL "
        sql += ",                         `min_response_processing_time_sec` REAL "
        sql += ",                         `max_response_processing_time_sec` REAL "
        sql += ",                         `sum_received_bytes` INTEGER "
        sql += ",                         `min_received_bytes` INTEGER "
        sql += ",                         `max_received_bytes` INTEGER "
        sql += ",                         `sum_sent_bytes` INTEGER "
        sql += ",                         `min_sent_bytes` INTEGER "
        sql += ",                         `max_sent_bytes` INTEGER "
        sql += ");"
        curs.execute(sql)

        self.commit()

    def _add_dimension(self, tablename, columname, value):
        sql = ""
        sql += "INSERT INTO `{}` (`{}`) VALUES (?);".format(tablename, columname)

        self._get_cursor().execute(sql, (value, ))
        id = self._get_cursor().lastrowid

        return id

    def _get_or_add_dimension(self, tablename, columname, value):
        sql = ""
        sql += "SELECT `id` FROM `{}` WHERE `{}` = ?".format(tablename, columname);

        res = self._get_cursor().execute(sql, (value, ))
        row = res.fetchone()
        if row:
            return row[0]
        else:
            return self._add_dimension( tablename, columname, value)

    def add_source(self, logname):
        sql = ""
        sql += "INSERT INTO `log_source` (`name`) VALUES (?);"

        self._get_cursor().execute(sql, (logname,))
        id = self._get_cursor().lastrowid

        return id

    def get_or_add_reqtype(self, value):
        return self._get_or_add_dimension("log_reqtype", "reqtype", value)

    def get_or_add_date(self, value):
        return self._get_or_add_dimension("log_date", "date", value)

    def get_or_add_hour(self, value):
        return self._get_or_add_dimension("log_hour", "hour", value)

    def get_or_add_minute(self, value):
        return self._get_or_add_dimension("log_minute", "minute", value)

    def get_or_add_elb(self, value):
        return self._get_or_add_dimension("log_elb", "elb", value)

    def get_or_add_method(self, value):
        return self._get_or_add_dimension("log_method", "method", value)

    def get_or_add_url(self, value):
        return self._get_or_add_dimension("log_url", "url", value)

    def get_or_add_http_version(self, value):
        return self._get_or_add_dimension("log_http_version", "http_version", value)

    def get_or_add_user_agent(self, value):
        return self._get_or_add_dimension("log_user_agent", "user_agent", value)

    def get_or_add_target_group(self, value):
        return self._get_or_add_dimension("log_target_group", "target_group", value)

    def get_or_add_domain(self, value):
        return self._get_or_add_dimension("log_domain", "domain", value)

    def get_or_add_target_address(self, value):
        return self._get_or_add_dimension("log_target_address", "target_address", value)

    def get_or_add_client_address(self, value):
        return self._get_or_add_dimension("log_client_address", "client_address", value)

    def save_url_stats(self, record):
        log_date_id = self.get_or_add_date(record["date"])
        log_hour_id = self.get_or_add_hour(record["hour"])
        log_url_id = self.get_or_add_url(record["url"])
        log_reqtype_id = self.get_or_add_reqtype(record["reqtype"])

        self._get_cursor().execute(STATS_URL_INSERT_SQL, (log_date_id, log_hour_id, log_url_id, log_reqtype_id,
                                   record["elb_status_code"],
                                   record["request_count"],
                                   record["sum_request_processing_time_sec"],
                                   record["min_request_processing_time_sec"],
                                   record["max_request_processing_time_sec"],
                                   record["sum_target_processing_time_sec"],
                                   record["min_target_processing_time_sec"],
                                   record["max_target_processing_time_sec"],
                                   record["sum_response_processing_time_sec"],
                                   record["min_response_processing_time_sec"],
                                   record["max_response_processing_time_sec"],
                                   record["sum_received_bytes"],
                                   record["min_received_bytes"],
                                   record["max_received_bytes"],
                                   record["sum_sent_bytes"],
                                   record["min_sent_bytes"],
                                   record["max_sent_bytes"]))

    def save_target_address_stats(self, record):
        log_date_id = self.get_or_add_date(record["date"])
        log_hour_id = self.get_or_add_hour(record["hour"])
        log_target_address_id = self.get_or_add_target_address(record["target_address"])

        self._get_cursor().execute(STATS_TARGET_ADDRESS_INSERT_SQL, (log_date_id, log_hour_id, log_target_address_id,
                                   record["target_port"],
                                   record["request_count"],
                                   record["sum_request_processing_time_sec"],
                                   record["min_request_processing_time_sec"],
                                   record["max_request_processing_time_sec"],
                                   record["sum_target_processing_time_sec"],
                                   record["min_target_processing_time_sec"],
                                   record["max_target_processing_time_sec"],
                                   record["sum_response_processing_time_sec"],
                                   record["min_response_processing_time_sec"],
                                   record["max_response_processing_time_sec"],
                                   record["sum_received_bytes"],
                                   record["min_received_bytes"],
                                   record["max_received_bytes"],
                                   record["sum_sent_bytes"],
                                   record["min_sent_bytes"],
                                   record["max_sent_bytes"]))

    def save_status_code_stats(self, record):
        log_date_id = self.get_or_add_date(record["date"])
        log_hour_id = self.get_or_add_hour(record["hour"])

        self._get_cursor().execute(STATS_STATUS_CODE_INSERT_SQL, (log_date_id, log_hour_id,
                                   record["target_status_code"],
                                   record["elb_status_code"],
                                   record["request_count"],
                                   record["sum_request_processing_time_sec"],
                                   record["min_request_processing_time_sec"],
                                   record["max_request_processing_time_sec"],
                                   record["sum_target_processing_time_sec"],
                                   record["min_target_processing_time_sec"],
                                   record["max_target_processing_time_sec"],
                                   record["sum_response_processing_time_sec"],
                                   record["min_response_processing_time_sec"],
                                   record["max_response_processing_time_sec"],
                                   record["sum_received_bytes"],
                                   record["min_received_bytes"],
                                   record["max_received_bytes"],
                                   record["sum_sent_bytes"],
                                   record["min_sent_bytes"],
                                   record["max_sent_bytes"]))

    def query_day_totals(self, datestr):
        sql = ""
        sql += "SELECT SUM(`tas`.`request_count`) `request_count` "
        sql += ",      SUM(`tas`.`sum_target_processing_time_sec`) `target_processing_time_sec` "
        sql += "FROM   `stats_target_address` `tas` "
        sql += ",      `log_date` `dte` "
        sql += "WHERE  `dte`.`id` = `tas`.`log_date_id` "
        sql += "AND    `dte`.`date` = ? "

        get_log().debug("query_day_totals: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_day_totals")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_day_totals")

        return result

    def query_target_address_stats(self, datestr):
        sql = ""
        sql += "SELECT   `hr`.`hour` `hour` "
        sql += ",        `tas`.`target_address` `target_address` "
        sql += ",        SUM(`sts`.`request_count`) `request_count` "
        sql += "FROM     `stats_target_address` `sts` "
        sql += ",        `log_target_address` `tas` "
        sql += ",        `log_date` `dte` "
        sql += ",        `log_hour` `hr` "
        sql += "WHERE    `dte`.`id` = `sts`.`log_date_id` "
        sql += "AND      `hr`.`id` = `sts`.`log_hour_id` "
        sql += "AND      `tas`.`id` = `sts`.`log_target_address_id` "
        sql += "AND      `dte`.`date` = ? "
        sql += "GROUP BY `hr`.`hour` "
        sql += ",        `tas`.`target_address` "
        sql += "ORDER BY `hr`.`hour`"
        sql += ",        `tas`.`target_address` "

        get_log().debug("query_target_address_stats: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_target_address_stats")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_target_address_stats")

        return result

    def query_top_10_url_by_count(self, datestr):
        sql = """select   `url`.`url` `url`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 group by `url`
                 order by `sum_request_count` desc
                 limit    10
              """

        get_log().debug("query_top_10_url_by_count: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_top_10_url_by_count")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_top_10_url_by_count")

        return result

    def query_top_x_url_by_time(self, datestr, limit=10):
        sql = """select  `url`.`url` `url`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 group by `url`
                 order by `sum_target_processing_time` desc
                 limit    ?
              """

        get_log().debug("query_top_10_url_by_time: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_top_10_url_by_time")
        result = self._get_cursor().execute(sql, (datestr, limit))
        get_log().info("END: Executing query_top_10_url_by_time")

        return result

    def query_top_x_url_by_time_excl_5xx(self, datestr, limit=10):
        sql = """select  `url`.`url` `url`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 and       `sts`.`elb_status_code` < 500
                 group by `url`
                 order by `sum_target_processing_time` desc
                 limit    ?
              """

        get_log().debug("query_top_x_url_by_time_excl_5xx: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_top_x_url_by_time_excl_5xx")
        result = self._get_cursor().execute(sql, (datestr, limit))
        get_log().info("END: Executing query_top_x_url_by_time_excl_5xx")

        return result

    def query_top_100_average_processing_time(self, datestr):
        sql = """select   `url`.`url` `url`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) /  sum(`sts`.`request_count`) `avg_target_processing_time`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 group by `url`
                 order by `avg_target_processing_time` desc
                 limit    100
                 """

        get_log().debug("query_top_10_url_by_time: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_top_10_url_by_time")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_top_10_url_by_time")

        return result

    def query_status_code(self, datestr):
        sql = """select   `sts`.`elb_status_code` `elb_status_code`
                 ,        `sts`.`target_status_code` `target_status_code`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) / sum(`sts`.`request_count`) `avg_target_processing_time`
                 from     `stats_status_code` `sts`
                 ,        `log_date` `dte`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `dte`.`date` = ?
                 group by `sts`.`target_status_code` 
                 ,        `sts`.`elb_status_code`
                 order by `elb_status_code`
                 ,        `target_status_code`
              """

        get_log().debug("query_status_code: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_status_code")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_status_code")

        return result

    def query_request_type(self, datestr):
        sql = """select   `rte`.`reqtype` `request_type`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) / sum(`sts`.`request_count`) `avg_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_reqtype` `rte`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `rte`.`id` = `sts`.`log_reqtype_id`
                 and      `dte`.`date` = ?
                 group by `rte`.`reqtype` 
                 order by `rte`.`reqtype`
              """

        get_log().debug("query_request_type: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_request_type")
        result = self._get_cursor().execute(sql, (datestr, ))
        get_log().info("END: Executing query_request_type")

        return result

    def query_max_date(self):
        # Joins stats_status_code on log_date and finds max date for which there is data
        # stats_status_code is chosen because it is the smallest table
        sql = """select max(`dte`.`date`) as `max_date`
                 from   `stats_status_code` `sts`
                 ,      `log_date` `dte`
                 where   `dte`.`id` = `sts`.`log_date_id` 
              """

        get_log().debug("query_max_date: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_max_date")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_max_date")
        row = result.fetchone()
        return row["max_date"]

    def query_min_date(self):
        # Joins stats_status_code on log_date and finds max date for which there is data
        # stats_status_code is chosen because it is the smallest table
        sql = """select min(`dte`.`date`) as `min_date`
                 from   `stats_status_code` `sts`
                 ,      `log_date` `dte`
                 where   `dte`.`id` = `sts`.`log_date_id` 
              """

        get_log().debug("query_max_date: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_max_date")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_max_date")
        row = result.fetchone()
        return row["min_date"]

    def query_url_stats_for_url_and_date(self, url, datestr):
        sql = """select  `url`.`url` `url`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 and      `url`.`url` = ?
                 group by `url`
              """

        get_log().debug("query_url_stats_for_url_and_date: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_url_stats_for_url_and_date")
        result = self._get_cursor().execute(sql, (datestr, url))
        get_log().info("END: Executing query_url_stats_for_url_and_date")

        return result

    def query_url_stats_for_url_and_date_excl_5xx(self, url, datestr):
        sql = """select  `url`.`url` `url`
                 ,        sum(`sts`.`request_count`) `sum_request_count`
                 ,        sum(`sts`.`sum_target_processing_time_sec`) `sum_target_processing_time`
                 from     `stats_url` `sts`
                 ,        `log_date` `dte`
                 ,        `log_url` `url`
                 where    `dte`.`id` = `sts`.`log_date_id`
                 and      `url`.`id` = `sts`.`log_url_id`
                 and      `dte`.`date` = ?
                 and      `url`.`url` = ?
                 and      `sts`.`elb_status_code` < 500
                 group by `url`
              """

        get_log().debug("query_url_stats_for_url_and_date_excl_5xx: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_url_stats_for_url_and_date_excl_5xx")
        result = self._get_cursor().execute(sql, (datestr, url))
        get_log().info("END: Executing query_url_stats_for_url_and_date_excl_5xx")

        return result

    def query_request_types(self):
        sql = """select   `rte`.`reqtype` `request_type`
                 from     `log_reqtype` `rte`
                 order by `rte`.`reqtype`
              """

        get_log().debug("query_request_types: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_request_types")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_request_types")

        return result
