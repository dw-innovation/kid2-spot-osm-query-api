import psycopg2
import psycopg2.extras
import psycopg2.pool
from flask import g

db_pool = None


def initialize_connection_pool(database_config):
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        user=database_config["user"],
        password=database_config["password"],
        host=database_config["host"],
        port=database_config["port"],
        database=database_config["name"],
    )


def get_db():
    if "db" not in g:
        g.db = db_pool.getconn()
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db_pool.putconn(db)
