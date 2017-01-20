from __future__ import absolute_import
import time
import hashlib
import lib.settings as settings
import lib.logger
log = lib.logger.get_logger(u'DB_Mysql')

import MySQLdb
from . import DB_Mysql


class DB_Mysql_Vardiff(DB_Mysql.DB_Mysql):

    def __init__(self):
        DB_Mysql.DB_Mysql.__init__(self)

    def import_shares(self, data):
        # Data layout
        # 0: worker_name,
        # 1: block_header,
        # 2: block_hash,
        # 3: difficulty,
        # 4: timestamp,
        # 5: is_valid,
        # 6: ip,
        # 7: self.block_height,
        # 8: self.prev_hash,
        # 9: invalid_reason,
        # 10: share_diff

        log.debug(u"Importing Shares")
        checkin_times = {}
        total_shares = 0
        best_diff = 0

        for k, v in enumerate(data):
            # for database compatibility we are converting our_worker to Y/N
            # format
            if v[5]:
                v[5] = u'Y'
            else:
                v[5] = u'N'

            self.execute(
                u"""
                INSERT INTO `shares`
                (time, rem_host, username, our_result,
                  upstream_result, reason, solution, difficulty)
                VALUES
                (FROM_UNIXTIME(%(time)s), %(host)s,
                  %(uname)s,
                  %(lres)s, 'N', %(reason)s, %(solution)s, %(difficulty)s)
                """,
                {
                    u"time": v[4],
                    u"host": v[6],
                    u"uname": v[0],
                    u"lres": v[5],
                    u"reason": v[9],
                    u"solution": v[2],
                    u"difficulty": v[3]
                }
            )

            self.dbh.commit()

    def found_block(self, data):
        # for database compatibility we are converting our_worker to Y/N format
        if data[5]:
            data[5] = u'Y'
        else:
            data[5] = u'N'

        # Check for the share in the database before updating it
        # Note: We can't use DUPLICATE KEY because solution is not a key

        self.execute(
            u"""
            Select `id` from `shares`
            WHERE `solution` = %(solution)s
            LIMIT 1
            """,
            {
                u"solution": data[2]
            }
        )

        shareid = self.dbc.fetchone()

        if shareid[0] > 0:
            # Note: difficulty = -1 here
            self.execute(
                u"""
                UPDATE `shares`
                SET `upstream_result` = %(result)s
                WHERE `solution` = %(solution)s
                AND `id` = %(id)s
                LIMIT 1
                """,
                {
                    u"result": data[5],
                    u"solution": data[2],
                    u"id": shareid[0]
                }
            )

            self.dbh.commit()
        else:
            self.execute(
                u"""
                INSERT INTO `shares`
                (time, rem_host, username, our_result,
                  upstream_result, reason, solution)
                VALUES
                (FROM_UNIXTIME(%(time)s), %(host)s,
                  %(uname)s,
                  %(lres)s, %(result)s, %(reason)s, %(solution)s)
                """,
                {
                    u"time": v[4],
                    u"host": v[6],
                    u"uname": v[0],
                    u"lres": v[5],
                    u"result": v[5],
                    u"reason": v[9],
                    u"solution": v[2]
                }
            )

            self.dbh.commit()

    def update_worker_diff(self, username, diff):
        log.debug(u"Setting difficulty for %s to %s", username, diff)

        self.execute(
            u"""
            UPDATE `pool_worker`
            SET `difficulty` = %(diff)s
            WHERE `username` = %(uname)s
            """,
            {
                u"uname": username,
                u"diff": diff
            }
        )

        self.dbh.commit()

    def clear_worker_diff(self):
        log.debug(u"Resetting difficulty for all workers")

        self.execute(
            u"""
            UPDATE `pool_worker`
            SET `difficulty` = 0
            """
        )

        self.dbh.commit()

    def get_workers_stats(self):
        self.execute(
            u"""
            SELECT `username`, `speed`, `last_checkin`, `total_shares`,
              `total_rejects`, `total_found`, `alive`, `difficulty`
            FROM `pool_worker`
            WHERE `id` > 0
            """
        )

        ret = {}

        for data in self.dbc.fetchall():
            ret[data[0]] = {
                u"username": data[0],
                u"speed": int(data[1]),
                u"last_checkin": time.mktime(data[2].timetuple()),
                u"total_shares": int(data[3]),
                u"total_rejects": int(data[4]),
                u"total_found": int(data[5]),
                u"alive": True if data[6] is 1 else False,
                u"difficulty": float(data[7])
            }

        return ret
