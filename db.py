#!/usr/bin/python3
import os
import sqlalchemy as s
import psycopg2 as ps

def pg_connect():
    '''
    Create connection to PostgreSQL for read or write purpose
    '''
    host = os.getenv('DBHOST')
    database = os.getenv('DBNAME')
    port = os.getenv('DBPORT')
    user = os.getenv('DBUSER')
    password = os.getenv('DBPASSWORD')

    engine = s.create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'. \
            format(user=user, password=password, host=host, port=port, database=database))

    return engine

def pg_execute(query):
    '''
    Create connection to PostgreSQL for other operations
    '''
    host = os.getenv('DBHOST')
    database = os.getenv('DBNAME')
    port = os.getenv('DBPORT')
    user = os.getenv('DBUSER')
    password = os.getenv('DBPASSWORD')

    conn = ps.connect(host=host, port=port, dbname=database, user=user, password=password)

    cursor = conn.cursor()
    cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

    print('Query executed successfully.')
    return

def grant_access():
    '''
    Grant access to metabase user
    '''
    dbname = os.getenv('DBNAME')
    with open('./sql/grant_access.sql') as f:
        grant_query = f.read().format(dbname=dbname)
        pg_execute(grant_query)
        f.close()
        print('Access granted.')

        return
