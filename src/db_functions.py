from datetime import datetime
import os
import logging

import pandas as pd
import sqlalchemy

PG_HOST = os.environ.get("PG_HOST")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

def insert_data(init_stock_file, table_name):
    logging.info(f"inserting values from {init_stock_file} into {table_name}")

    engine = sqlalchemy.create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}')
    engine.connect()

    df = pd.read_csv(init_stock_file)
    df['added_by'] = 'system'
    df['added_date'] = pd.to_datetime(datetime.now())
    df.set_index('symbol', inplace=True)

    if sqlalchemy.inspect(engine).has_table(table_name):
        df_existing = pd.read_sql(f"SELECT * from {table_name}", engine)
        logging.info(f'found {df.shape[0]} existing rows in {table_name}')
        # remove the rows that already exists in the table from the df
        df_existing.set_index('symbol', inplace=True)
        df = df[~df.index.isin(df_existing.index)]
    else:
        df.head(n=0).to_sql(name=table_name, con=engine)

    # insert remaining values into the table
    if df.empty:
        logging.info('no new values to insert')
    else:
        df.to_sql(name=table_name, con=engine, if_exists='append')
        logging.info(f'inserted {df.shape[0]} rows')