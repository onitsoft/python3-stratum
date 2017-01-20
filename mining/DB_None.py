from __future__ import absolute_import
import stratum.logger
log = stratum.logger.get_logger(u'None')


class DB_None(object):

    def __init__(self):
        log.debug(u"Connecting to DB")

    def updateStats(self, averageOverTime):
        log.debug(u"Updating Stats")

    def import_shares(self, data):
        log.debug(u"Importing Shares")

    def found_block(self, data):
        log.debug(u"Found Block")

    def get_user(self, id_or_username):
        log.debug(u"Get User")

    def list_users(self):
        log.debug(u"List Users")

    def delete_user(self, username):
        log.debug(u"Deleting Username")

    def insert_user(self, username, password):
        log.debug(u"Adding Username/Password")

    def update_user(self, username, password):
        log.debug(u"Updating Username/Password")

    def check_password(self, username, password):
        log.debug(u"Checking Username/Password")
        return True

    def update_pool_info(self, pi):
        log.debug(u"Update Pool Info")

    def clear_worker_diff(self):
        log.debug(u"Clear Worker Diff")

    def get_pool_stats(self):
        log.debug(u"Get Pool Stats")
        ret = {}
        return ret

    def get_workers_stats(self):
        log.debug(u"Get Workers Stats")
        ret = {}
        return ret

    def check_tables(self):
        log.debug(u"Checking Tables")

    def close(self):
        log.debug(u"Close Connection")
