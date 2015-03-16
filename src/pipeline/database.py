import MySQLdb
import os
from src.pipeline import consts

# ''' Connect to DB '''
class DB(object):

    def __init__(self, host=None, user=None, passwd=None, dbname=None):
        if not host:
            host = os.environ.get(consts.PP_DB_HOST)
        if not user:
            user = os.environ.get(consts.PP_DB_USER)
        if not passwd:
            passwd = os.environ.get(consts.PP_DB_PW, None)
        if not dbname:
            dbname = os.environ.get(consts.PP_DB_NAME)

        self.db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dbname)
        self.cursor = self.db.cursor()

    def make_dict(self, table, key, value, overide={}, type=None, where="", slice=None):
        if where:
            where = "WHERE {}".format(where)
        self.cursor.execute("select %s, %s from %s %s" % (key, value, table, where))
        data = {str(r[0]):r[1] for r in self.cursor.fetchall()}

        overide = {str(k) : v for k,v in overide.items()}
        data = dict(data.items() + overide.items())
        return data

    def close(self):
        self.cursor.close()
