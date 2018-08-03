import os.path
import sqlite3
import logging

LOGGER = None

sql = ""
sql += "INSERT INTO `log_entry`  "
sql += "(                         `log_source_id` "
sql += ",                         `log_reqtype_id` "
sql += ",                         `log_date_id` "
sql += ",                         `log_hour_id` "
sql += ",                         `log_minute_id` "
sql += ",                         `log_elb_id` "
sql += ",                         `log_method_id` "
sql += ",                         `log_url_id` "
sql += ",                         `log_http_version_id` "
sql += ",                         `log_user_agent_id` "
sql += ",                         `log_target_group_id` "
sql += ",                         `log_domain_id` "
sql += ",                         `log_target_address_id` "
sql += ",                         `log_client_address_id` "
sql += ",                         `seconds` "
sql += ",                         `microseconds` "
sql += ",                         `timestamp` "
sql += ",                         `request_type` "
sql += ",                         `elb` "
sql += ",                         `client_port` "
sql += ",                         `target_port` "
sql += ",                         `request_processing_time_sec` "
sql += ",                         `target_processing_time_sec` "
sql += ",                         `response_processing_time_sec` "
sql += ",                         `elb_status_code` "
sql += ",                         `target_status_code` "
sql += ",                         `received_bytes` "
sql += ",                         `sent_bytes` "
sql += ",                         `request_query` "
sql += ",                         `request_creation_time` "
sql += ",                         `actions_executed` "
sql += ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

ENTRY_INSERT_SQL = sql


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
        sql += "CREATE TABLE `log_source` (`id` INTEGER PRIMARY KEY "
        sql += ",                          `name` TEXT);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_source_name` ON `log_source`(`name`);"
        curs.execute(sql)

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
        sql += "CREATE TABLE `log_minute` (`id` INTEGER PRIMARY KEY "
        sql += ",                        `minute` INTEGER);"
        curs.execute(sql)

        sql = ""
        sql += "CREATE UNIQUE INDEX `ux_log_minute_minute` ON `log_minute`(`minute`);"
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
        sql += "CREATE TABLE `log_entry` (`id` INTEGER PRIMARY KEY "
        sql += ",                         `log_source_id` INTEGER "
        sql += ",                         `log_reqtype_id` INTEGER "
        sql += ",                         `log_elb_id` INTEGER "
        sql += ",                         `log_date_id` INTEGER "
        sql += ",                         `log_hour_id` INTEGER "
        sql += ",                         `log_minute_id` INTEGER "
        sql += ",                         `log_method_id` INTEGER "
        sql += ",                         `log_url_id` INTEGER "
        sql += ",                         `log_http_version_id` INTEGER "
        sql += ",                         `log_user_agent_id` INTEGER "
        sql += ",                         `log_target_group_id` INTEGER "
        sql += ",                         `log_domain_id` INTEGER "
        sql += ",                         `log_target_address_id` INTEGER "
        sql += ",                         `log_client_address_id` INTEGER "
        sql += ",                         `seconds` INTEGER "
        sql += ",                         `microseconds` INTEGER "
        sql += ",                         `timestamp` TEXT "
        sql += ",                         `request_type` TEXT "
        sql += ",                         `elb` TEXT "
        sql += ",                         `client_port` INTEGER "
        sql += ",                         `target_port` INTEGER "
        sql += ",                         `request_processing_time_sec` REAL "
        sql += ",                         `target_processing_time_sec` REAL "
        sql += ",                         `response_processing_time_sec` REAL "
        sql += ",                         `elb_status_code` INTEGER "
        sql += ",                         `target_status_code` INTEGER "
        sql += ",                         `received_bytes` INTEGER "
        sql += ",                         `sent_bytes` INTEGER "
        sql += ",                         `request_query` TEXT "
        sql += ",                         `request_creation_time` TEXT "
        sql += ",                         `actions_executed` TEXT "
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

    def get_or_add_target_address(self, value):
        return self._get_or_add_dimension("log_target_address", "target_address", value)

    def get_or_add_client_address(self, value):
        return self._get_or_add_dimension("log_client_address", "client_address", value)

    def get_or_add_domain(self, value):
        return self._get_or_add_dimension("log_domain", "domain", value)

    def save_record(self, log_source_id, record):
        log_reqtype_id = self.get_or_add_reqtype(record["type"])
        log_date_id = self.get_or_add_date(record["datepart"])
        log_hour_id = self.get_or_add_hour(record["hourpart"])
        log_minute_id = self.get_or_add_minute(record["minutepart"])
        log_elb_id = self.get_or_add_elb(record["elb"])
        log_method_id = self.get_or_add_method(record["request_method"])
        log_url_id = self.get_or_add_url(record["request_url"])
        log_http_version_id = self.get_or_add_http_version(record["http_version"])
        log_user_agent_id = self.get_or_add_user_agent(record["user_agent"])
        log_target_group_id = self.get_or_add_target_group(record["target_group_arn"])
        log_domain_id = self.get_or_add_domain(record["domain_name"])
        log_target_address_id = self.get_or_add_target_address(record["target_ip"])
        log_client_address_id = self.get_or_add_client_address(record["client_ip"])

        self._get_cursor().execute(ENTRY_INSERT_SQL, (log_source_id, log_reqtype_id, log_date_id, log_hour_id,
                                                      log_minute_id, log_elb_id, log_method_id, log_url_id,
                                                      log_http_version_id, log_user_agent_id, log_target_group_id,
                                                      log_domain_id, log_target_address_id, log_client_address_id,
                                                      record["secondspart"], record["microsecondspart"],
                                                      record["timestamp"], record["type"], record["elb"],
                                                      record["client_port"],
                                                      record["target_port"], record["request_processing_time"],
                                                      record["target_processing_time"],
                                                      record["response_processing_time"], record["elb_status_code"],
                                                      record["target_status_code"], record["received_bytes"],
                                                      record["sent_bytes"], record["request_query"],
                                                      record["request_creation_time"], record["actions_executed"]))

    def query_url_stats(self):
        sql = ""
        sql += "SELECT   `url`.`url`  `url`  "
        sql += ",        `dte`.`date` `date` "
        sql += ",        `hr`.`hour`  `hour` "
        sql += ",        `ety`.`elb_status_code`  `elb_status_code` "
        sql += ",        COUNT(*)     `request_count` "
        sql += ",        SUM(`request_processing_time_sec`) `sum_request_processing_time_sec` "
        sql += ",        MIN(`request_processing_time_sec`) `min_request_processing_time_sec` "
        sql += ",        MAX(`request_processing_time_sec`) `max_request_processing_time_sec` "
        sql += ",        SUM(`target_processing_time_sec`) `sum_target_processing_time_sec` "
        sql += ",        MIN(`target_processing_time_sec`) `min_target_processing_time_sec` "
        sql += ",        MAX(`target_processing_time_sec`) `max_target_processing_time_sec` "
        sql += ",        SUM(`response_processing_time_sec`) `sum_response_processing_time_sec` "
        sql += ",        MIN(`response_processing_time_sec`) `min_response_processing_time_sec` "
        sql += ",        MAX(`response_processing_time_sec`) `max_response_processing_time_sec` "
        sql += ",        SUM(`received_bytes`) `sum_received_bytes` "
        sql += ",        MIN(`received_bytes`) `min_received_bytes` "
        sql += ",        MAX(`received_bytes`) `max_received_bytes` "
        sql += ",        SUM(`sent_bytes`) `sum_sent_bytes` "
        sql += ",        MIN(`sent_bytes`) `min_sent_bytes` "
        sql += ",        MAX(`sent_bytes`) `max_sent_bytes` "
        sql += "FROM     `log_url`   `url` "
        sql += ",        `log_date`  `dte` "
        sql += ",        `log_hour`  `hr` "
        sql += ",        `log_entry` `ety` "
        sql += "WHERE    `url`.`id` = `ety`.`log_url_id`"
        sql += "AND      `dte`.`id` = `ety`.`log_date_id`"
        sql += "AND      `hr`.`id`  = `ety`.`log_hour_id`"
        sql += "GROUP BY `url`.`url` "
        sql += ",        `dte`.`date` "
        sql += ",        `hr`.`hour`  "

        get_log().debug("query_url_stats: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_url_stats")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_url_stats")

        return result

    def query_target_address_stats(self):
        sql = ""
        sql += "SELECT   `tas`.`target_address`  `target_address`  "
        sql += ",        `ety`.`target_port` `target_port` "
        sql += ",        `dte`.`date` `date` "
        sql += ",        `hr`.`hour`  `hour` "
        sql += ",        COUNT(*)     `request_count` "
        sql += ",        SUM(`request_processing_time_sec`) `sum_request_processing_time_sec` "
        sql += ",        MIN(`request_processing_time_sec`) `min_request_processing_time_sec` "
        sql += ",        MAX(`request_processing_time_sec`) `max_request_processing_time_sec` "
        sql += ",        SUM(`target_processing_time_sec`) `sum_target_processing_time_sec` "
        sql += ",        MIN(`target_processing_time_sec`) `min_target_processing_time_sec` "
        sql += ",        MAX(`target_processing_time_sec`) `max_target_processing_time_sec` "
        sql += ",        SUM(`response_processing_time_sec`) `sum_response_processing_time_sec` "
        sql += ",        MIN(`response_processing_time_sec`) `min_response_processing_time_sec` "
        sql += ",        MAX(`response_processing_time_sec`) `max_response_processing_time_sec` "
        sql += ",        SUM(`received_bytes`) `sum_received_bytes` "
        sql += ",        MIN(`received_bytes`) `min_received_bytes` "
        sql += ",        MAX(`received_bytes`) `max_received_bytes` "
        sql += ",        SUM(`sent_bytes`) `sum_sent_bytes` "
        sql += ",        MIN(`sent_bytes`) `min_sent_bytes` "
        sql += ",        MAX(`sent_bytes`) `max_sent_bytes` "
        sql += "FROM     `log_target_address`   `tas` "
        sql += ",        `log_date`  `dte` "
        sql += ",        `log_hour`  `hr` "
        sql += ",        `log_entry` `ety` "
        sql += "WHERE    `tas`.`id` = `ety`.`log_target_address_id`"
        sql += "AND      `dte`.`id` = `ety`.`log_date_id`"
        sql += "AND      `hr`.`id`  = `ety`.`log_hour_id`"
        sql += "GROUP BY `tas`.`target_address` "
        sql += ",        `ety`.`target_port` "
        sql += ",        `dte`.`date` "
        sql += ",        `hr`.`hour`  "

        get_log().debug("query_target_stats: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_target_stats")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_target_stats")

        return result

    def query_status_code_stats(self):
        sql = """SELECT   `ety`.`target_status_code` `target_status_code`
                 ,        `ety`.`elb_status_code` `elb_status_code`
                 ,        `dte`.`date` `date`
                 ,        `hr`.`hour`  `hour`
                 ,        COUNT(*)     `request_count`
                 ,        SUM(`request_processing_time_sec`) `sum_request_processing_time_sec`
                 ,        MIN(`request_processing_time_sec`) `min_request_processing_time_sec`
                 ,        MAX(`request_processing_time_sec`) `max_request_processing_time_sec`
                 ,        SUM(`target_processing_time_sec`) `sum_target_processing_time_sec`
                 ,        MIN(`target_processing_time_sec`) `min_target_processing_time_sec`
                 ,        MAX(`target_processing_time_sec`) `max_target_processing_time_sec`
                 ,        SUM(`response_processing_time_sec`) `sum_response_processing_time_sec`
                 ,        MIN(`response_processing_time_sec`) `min_response_processing_time_sec`
                 ,        MAX(`response_processing_time_sec`) `max_response_processing_time_sec`
                 ,        SUM(`received_bytes`) `sum_received_bytes`
                 ,        MIN(`received_bytes`) `min_received_bytes`
                 ,        MAX(`received_bytes`) `max_received_bytes`
                 ,        SUM(`sent_bytes`) `sum_sent_bytes`
                 ,        MIN(`sent_bytes`) `min_sent_bytes`
                 ,        MAX(`sent_bytes`) `max_sent_bytes`
                 FROM     `log_date`  `dte`
                 ,        `log_hour`  `hr`
                 ,        `log_entry` `ety`
                 WHERE    `dte`.`id` = `ety`.`log_date_id`
                 AND      `hr`.`id`  = `ety`.`log_hour_id`
                 GROUP BY `ety`.`target_status_code`
                 ,        `ety`.`elb_status_code`
                 ,        `dte`.`date` 
                 ,        `hr`.`hour`  
              """

        get_log().debug("query_status_code_stats: query = {}".format(sql))

        get_log().info("BEGIN: Executing query_status_code_stats")
        result = self._get_cursor().execute(sql)
        get_log().info("END: Executing query_status_code_stats")

        return result
