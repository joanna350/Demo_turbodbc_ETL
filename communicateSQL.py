import time
import os
import logging
import pandas as pd
import numpy as np
import pyodbc
import turbodbc
import dask.dataframe as da
from env import connect_to_sql, dest
from utilities import getlist, load_by_ext


def get_all_df(testdrive=1):
    """
    retrieves all dfs from the input, unify dtype format
    saves as parquet for intermediary storage
    returns df for other processing
    """
    if not testdrive:
        resultlist = getlist(dest)
        logging.debug(f'for non-testdrive, read from {resultlist}')
    else:
        resultlist = ['filename.xlsx']
        logging.debug(f'for testdrive, read only {resultlist}')

    result = []
    logging.debug(f'total count of files {len(resultlist)}')

    for fn in resultlist:
        logging.debug(f'==============read file {fn}')
        fp = os.path.join(dest, fn)
        df = load_by_ext(fp)
        df.insert(0, 'Filename', fn) # add for info
        logging.debug(f'==============column length check {len(df.columns)}')
        df = type_check(df)
        result.append(df)
    logging.debug(f'==============concat all df {len(result)}')
    total_df = pd.concat(result)

    ddf = da.from_pandas(total_df, chunksize = 5000000)
    save_dir = 'interim/'
    ddf.to_parquet(save_dir)

    return total_df


def turbo_write(df=[], table='default_table', cnxnstr=connect_to_sql):
    
    start_time = time.time()
    if len(df) == 0:
        df = get_all_df()
        df = type_check(df)
    logging.info(f'{len(df.columns)}')
    connection = turbodbc.connect(connection_string=cnxnstr)
    logging.debug(f'==========connection is created, now to writing')

    # prepare columns
    columns = '('
    columns += ', '.join(df.columns)
    columns += ')'
    logging.debug('columns')
    logging.debug(f'{columns}')

    # prepare value placeholders
    val_placeholder = ['?' for _ in df.columns]
    sql_val = '('
    sql_val += ', '.join(val_placeholder)
    sql_val += ')'
    logging.debug('value placeholder')
    logging.debug(sql_val)

    # write sql query for turbodbc
    sql = f"""
            INSERT INTO {table} {columns}
            VALUES {sql_val}
            """

    # errors if not numpy array of values
    values_df = [np.array(df[col].values) for col in df.columns]
    
    # insert data
    with connection.cursor() as cursor:
        cursor.executemanycolumns(sql, values_df)
        connection.commit()

    connection.close()
    end_time = time.time()
    logging.info(f'completed in {end_time - start_time} s')


def type_check(df):
    """
    the process of writing to sql server will error if dtype mismatches

    Args:
        df: original pd dataframe 

    Returns:   
        df: pd dataframe with uniformized dtypes
    """
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())

    float_check = ['col_A', 'col_B', 'col_C']
    for c in df.columns:
        logging.info(f'prepare column {c} conversion at {df[c].dtypes}')
        if c in float_check:
            df = sql_dtype_handler(df, c, float)
        else: # let's say string
            df = sql_dtype_handler(df, c, str)
    return df


def sql_dtype_handler(df, c, dtype):
    """
    Args:
        df: pd dataframe where dtpye conversion is necessary by column
        c: column name
        dtype: the type that the column has to convert into

    Returns:
        prepared df    
    """
    # ensure there is no empty cell with mismatching content as otherwise they will error
   if dtype in [int, float] and df[c].isnull().values.any():
        df[c] = df[c].fillna(0)
        df[c] = df[c].replace('', 0)
        logging.debug(f'define the rest that needs handling {df[df[c] != 0][c]}')
    elif dtype in [str] and df[c].isnull().values.any():
        df[c] = df[c].fillna('')
    
    if df[c].dtypes != dtype:
        if dtype == float:
            df[c] = df[c].astype(np.float64) 
        else:
            df[c] = df[c].astype(dtype)
        logging.debug(f'per column {c}, {df[c].dtype} data type saved')

    return df 


