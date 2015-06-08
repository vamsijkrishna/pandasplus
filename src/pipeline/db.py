# db.py
from src.pipeline.consts import *
import os
import psycopg2

def write_table(file_path, table_name, cols, database_settings):
    user = database_settings[USER]
    pw = os.environ.get(database_settings[PW_ENV_VAR], None)
    host = database_settings[HOST]
    dbname = database_settings[DB_NAME]
    conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(dbname, user, host, pw))
    cur = conn.cursor()

    cols = ",".join(cols)
    f = open(file_path)
    cmd = "COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER '	';".format(table_name, cols)
    print cmd
    res = cur.copy_expert(cmd, f)
    conn.commit()
    print cur
    print res
    # con_url = 'postgresql://{}:{}@{}/{}'.format(user, pw, host, dbname)
    # engine = create_engine(con_url, pool_recycle=3600)
    # df.to_sql(name, engine, index=False, chunksize=10000)

if __name__ == '__main__':
    settings = {
        USER: "postgres",
        HOST: "192.168.0.104",
        PW_ENV_VAR: "DATAUSA_PW",
        DB_NAME: "postgres"
    }
    write_table("/tmp/test.tsv", "mytest", settings)