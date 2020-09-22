import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import configparser
config = configparser.ConfigParser()
config.read('conf.cfg')


def create_database():
    # connect to default database
    conn = psycopg2.connect(f"host={config.get('POSTGRES','host')} dbname={config.get('POSTGRES','dbname')} user={config.get('POSTGRES','user')} password={config.get('POSTGRES','password')} port={config.get('POSTGRES','port')}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(f"host={config.get('POSTGRES','host')} dbname=sparkifydb user={config.get('POSTGRES','user')} password={config.get('POSTGRES','password')} port={config.get('POSTGRES','port')}")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
