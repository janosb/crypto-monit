import psycopg2
from sqlalchemy import create_engine
from tg_api_config import *

conn = psycopg2.connect(database = pg_db, user = pg_user, password = pg_password, port = pg_port, host = pg_host)
cursor = conn.cursor()

engine = create_engine("postgres://%s:%s@%s:%s/%s" % (pg_user, pg_password, pg_host, pg_port, pg_db))
slqa_conn = engine.connect()


