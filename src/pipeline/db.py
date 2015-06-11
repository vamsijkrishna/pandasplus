# db.py
from src.pipeline.consts import *
import os
import psycopg2
import pandas.io.sql as pd_sql


import pandas as pd
import psycopg2

from sqlalchemy import create_engine

from pandas.io.sql import get_schema
import cStringIO

# def has_upper_case(x):
#     return x.lower() != x

# def insert_monkey(self, chunksize=None):
#     conn = self.pd_sql.engine.raw_connection()
#     cursor = conn.cursor()
#     df = self.frame
#     nrows = len(df)

#     if nrows == 0:
#         return
#     cols = ",".join(['"{}"'.format(c) if has_upper_case(c) else c for c in df.columns])

#     target_name = self.name
#     if self.schema:
#         target_name = "{}.{}".format(self.schema, self.name)

#     # Chunk the dataframe 
#     chunks = int(nrows / chunksize) + 1
#     for i in range(chunks):
#         start_i = i * chunksize
#         end_i = min((i + 1) * chunksize, nrows)
#         if start_i >= end_i:
#             break
#         tmp_df = df.iloc[start_i:end_i]
#         # Write df to_csv in memory
#         output = cStringIO.StringIO()
#         tmp_df.to_csv(output, sep="\t", index=self.index)
#         output.seek(0)
#         cmd = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER '\t';".format(target_name, cols)
#         res = cursor.copy_expert(cmd, output)
#         conn.commit()

def make_schema(df, name, database_settings):
    user = database_settings[USER]
    pw = os.environ.get(database_settings[PW_ENV_VAR], None)
    host = database_settings[HOST]
    dbname = database_settings[DB_NAME]
    conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(dbname, user, host, pw))
    cur = conn.cursor()
    sql_str = get_schema(df, name).replace(r'"{}"'.format(name), name)
    try:
        cur.execute(sql_str)
        conn.commit()
    except psycopg2.ProgrammingError:
        print "[WARNING] Table already exists..."
    finally:
        conn.close()

def write_table(file_path, table_name, cols, database_settings, encoding):
    user = database_settings[USER]
    pw = os.environ.get(database_settings[PW_ENV_VAR], None)
    host = database_settings[HOST]
    dbname = database_settings[DB_NAME]
    conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(dbname, user, host, pw))
    cur = conn.cursor()

    cols = ",".join(cols)
    f = open(file_path)
    print "Importing from", file_path, "..."
    cmd = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER '	';".format(table_name, cols)
    print cmd
    if not encoding:
        cur.execute("SET CLIENT_ENCODING TO 'LATIN1';"); # -- TODO only if encoding is None
    res = cur.copy_expert(cmd, f)
    conn.commit()
    # con_url = 'postgresql://{}:{}@{}/{}'.format(user, pw, host, dbname)
    # engine = create_engine(con_url, pool_recycle=3600)
    # df.to_sql(name, engine, index=False, chunksize=10000)
    # conn.close()

if __name__ == '__main__':
    settings = {
        USER: "postgres",
        HOST: "192.168.0.104",
        PW_ENV_VAR: "DATAUSA_PW",
        DB_NAME: "postgres"
    }
    write_table("/tmp/test.tsv", "mytest", settings)