#!/usr/bin/python3
import os
import sqlalchemy as s

def pg_connect():
    '''
    Create connection to PostgreSQL
    '''
    host = os.getenv('DBHOST')
    database = os.getenv('DBNAME')
    port = os.getenv('DBPORT')
    user = os.getenv('DBUSER')
    password = os.getenv('DBPASSWORD')

    engine = s.create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'. \
            format(user=user, password=password, host=host, port=port, database=database))
    print('Connected!')
    return engine
