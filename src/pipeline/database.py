# import MySQLdb

# ''' Connect to DB '''
# class DB(object):

#     def __init__(self, host="127.0.0.1", user="root", passwd="", dbname="dataviva2"):
#         self.db = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dbname)
#         self.cursor = self.db.cursor()

#     def make_dict(self, table, key, value, overide={}, type=None):
#         self.cursor.execute("select %s, %s from %s " % (key, value, table))
#         data = {str(r[0]):r[1] for r in self.cursor.fetchall()}

#         overide = {str(k) : v for k,v in overide.items()}
#         data = dict(data.items() + overide.items())

#         return data
class DB(object):
    pass