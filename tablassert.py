''' Skye Goetz (ISB) 09/11/2024 '''

import os
import sys
import copy
import math
import yaml
import sqlite3
import logging
import subprocess
import polars as pl
from urllib.request import urlretrieve

# Set up environment variables and file paths
BIN = os.environ['tablassert']
CONFIG_FILE = sys.argv[1]
SOURCE_DATA = sys.argv[2]
LOG_PATH = os.path.join(SOURCE_DATA, 'log')

# Create log directory and set up logging configuration
os.makedirs(LOG_PATH, exist_ok=True)
if os.path.isfile(os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log')):
    os.remove(os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log'))
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s\t%(levelname)s\t%(message)s',
    filename=os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log'), filemode='w'
)

# Set up database connections and adjust SQLite settings
DB_MAP = os.environ['tablassertDBMap']
DB_RES = os.environ['tablassertDBRes']
DB_PREF = os.environ['tablassertDBPref']
DB_HASH = os.environ['tablassertDBHash']

conn_res = sqlite3.connect(DB_RES)
conn_hash = sqlite3.connect(DB_HASH)
conn_map = sqlite3.connect(DB_MAP)
conn_pref = sqlite3.connect(DB_PREF)

conn_res.execute('PRAGMA synchronous = OFF')
conn_hash.execute('PRAGMA synchronous = OFF')
conn_map.execute('PRAGMA synchronous = OFF')
conn_pref.execute('PRAGMA synchronous = OFF')

conn_res.execute('PRAGMA cache_size = -64000')
conn_hash.execute('PRAGMA cache_size = -64000')
conn_map.execute('PRAGMA cache_size = -64000')
conn_pref.execute('PRAGMA cache_size = -64000')

# Function to open and read the file, based on its extension
def openIt(PARAM):
    os.makedirs(os.path.dirname(PARAM['data_location']['path_to_file']), exist_ok=True)
    if not os.path.isfile(PARAM['data_location']['path_to_file']):
        urlretrieve(PARAM['provenance']['table_url'], PARAM['data_location']['path_to_file'])
    EXT = os.path.splitext(os.path.basename(PARAM['data_location']['path_to_file']))[1]
    if EXT in ['.xls', '.xlsx', '.XLS', '.XLSX']:
        source = pl.read_excel(PARAM['data_location']['path_to_file'], sheet_name=PARAM['data_location']['sheet_to_use'], has_header=False)
        source = source.slice((int(PARAM['data_location']['first_line']) - 1), PARAM['data_location']['last_line'] - 1)
    elif EXT in ['.csv', '.tsv', 'txt']:
        source = pl.read_csv(PARAM['data_location']['path_to_file'], separator=PARAM['data_location']['delimiter'])
    else:
        print(f'Sorry. Tablassert doesn\'t yet support {EXT} files.')
    return source, EXT

# Function to process attributes of the DataFrame
def attributes(source, PARAM):
    for attribute, config in PARAM['attributes'].items():
        if attribute in source.columns and config.get('column_name') != attribute:
            source = source.drop(attribute)
        if 'value' in config:
            source = source.with_columns(pl.lit(config['value']).alias(attribute))
        else:
            source = source.rename({config['column_name']: attribute})
        if 'math' in config:
            for op in config['math']:
                operation = getattr(math, op['operation'])
                val = float(op.get('parameter', '0'))
                order = bool(op.get('order_last'))
                if order:
                    values = [operation(float(x), val) if x is not None else None for x in source[attribute].to_list()]
                    source = source.with_columns(pl.Series(attribute, values).alias(attribute))
                else:
                    values = [operation(val, float(x)) if x is not None else None for x in source[attribute].to_list()]
                    source = source.with_columns(pl.Series(attribute, values).alias(attribute))
    return source

# Function to convert column indices to Excel column names
def getXlsxColumnName(n):
    if n < 26:
        return chr(n + 65)
    else:
        first_letter = chr((n - 26) // 26 + 65)
        second_letter = chr((n % 26) + 65)
        return first_letter + second_letter

# Function to rename columns with Excel-style letters
def ascii(source):
    new_column_names = [getXlsxColumnName(i) for i in range(len(source.columns))]
    source = source.rename({old_name: new_name for old_name, new_name in zip(source.columns, new_column_names)})
    return source

# Function to reindex DataFrame based on specified parameters
def reindexIt(source, PARAM):
    for op in PARAM['reindex']:
        val = float(
            op['value']) if isinstance(op['value'], (int, float)) \
            else str(op['value']
        )
        cast_type = float if 'greater_than_or_equal_to' in op['mode'] or \
            'less_than_or_equal_to' in op['mode'] else str
        col = source[op['column']].cast(cast_type)
        if 'greater_than_or_equal_to' in op['mode']: 
            source = source.filter(pl.col(op['column']) >= val)
        if 'less_than_or_equal_to' in op['mode']: 
            source = source.filter(pl.col(op['column']) <= val)
        if 'if_equals' in op['mode']: 
            source = source.filter(pl.col(op['column']) != str(val))
    return source

# Function to format columns in the DataFrame based on various parameters
def nodeColumnFormat(source, PARAM, col):
    if 'fill_values' in PARAM[col]:
        source = source.with_columns(pl.col(col).fill_null(strategy=PARAM[col]['fill_values']))
    if 'explode_column' in PARAM[col]:
        source = source.with_columns(pl.col(col).str.split(PARAM[col]['explode_column'])).explode(col)
    if 'regex_replacements' in PARAM[col]:
        for replacement in PARAM[col]['regex_replacements']:
            repl = replacement['replacement'] if replacement['replacement'] is not None else ''
            source = source.with_columns(
                pl.col(col).str.replace_all(replacement['pattern'], repl).alias(col)
            )
    if 'prefix' in PARAM[col]:
        for prefix in PARAM[col]['prefix']:
            source = source.with_columns(
                (pl.lit(prefix['prefix']) + pl.col(col).cast(pl.Utf8)).alias(col)
            )
    return source

# Function to map values without considering classes
def classlessDBResHash(val):
    try:
        cursor_res = conn_res.cursor()
        cursor_res.execute('SELECT CURIE FROM SYNONYMS WHERE SYNONYM = ? LIMIT 1', (val,))
        results_res = cursor_res.fetchall()
        if results_res:
            logging.info(f'{DB_RES}\t{val} BECAME {results_res[0][0]}\tCLASSLESS\tFIRST TAKEN')
            return results_res[0][0]
        if not os.path.isfile(os.path.join(BIN, 'hashingAndRegex')):
            os.system(f'g++ -std=c++11 -o {BIN}/hashingAndRegex {BIN}/hashingAndRegex.cpp -lssl -lcrypto')
        result = subprocess.run(os.path.join(BIN, 'hashingAndRegex'), input=val, text=True, capture_output=True, check=True)
        hashed_val = result.stdout.strip()
        cursor_hash = conn_hash.cursor()
        cursor_hash.execute('SELECT CURIE FROM HASHES WHERE HASH = ? LIMIT 1', (hashed_val,))
        results_hash = cursor_hash.fetchall()
        if results_hash:
            logging.info(f'{DB_HASH}\t{val} BECAME {results_hash[0][0]}\tCLASSLESS\tHASHED\tFIRST TAKEN')
            return results_hash[0][0]
        else :
            logging.warning(f'{DB_RES}\t{val} FAILED TO MAP')
            return None
    except Exception as e:
        logging.critical(f'{DB_HASH}\t{val} BROKE CLASSLESS MAPPING : {e}')
        return None

# Function to map values considering their classes
def classedDBResHash(val, PARAM, col):
    try:
        cursor_res = conn_res.cursor()
        cursor_res.execute('SELECT CURIE FROM SYNONYMS WHERE SYNONYM = ? LIMIT 1', (val,))
        results_res = cursor_res.fetchall()
        if results_res:
            cursor_pref = conn_pref.cursor()
            cursor_pref.execute('SELECT CATEGORY FROM NAMES WHERE CURIE = ? LIMIT 1', (results_res[0][0],))
            results_res_category = cursor_pref.fetchall()
            if results_res_category[0][0] in PARAM[col]['expected_classes']:
                logging.info(f'{DB_RES}\t{val} BECAME {results_res[0][0]}\tCLASSED\tFIRST TAKEN')
                return results_res[0][0]
        if not os.path.isfile(os.path.join(BIN, 'hashingAndRegex')):
            os.system(f'g++ -std=c++11 -o {BIN}/hashingAndRegex {BIN}/hashingAndRegex.cpp -lssl -lcrypto')
        result = subprocess.run(os.path.join(BIN, 'hashingAndRegex'),input=val, text=True, capture_output=True, check=True)
        hashed_val = result.stdout.strip()
        cursor_hash = conn_hash.cursor()
        cursor_hash.execute('SELECT CURIE FROM HASHES WHERE HASH = ? LIMIT 1', (hashed_val,))
        results_hash = cursor_hash.fetchall()
        if results_hash:
            cursor_pref = conn_pref.cursor()
            cursor_pref.execute('SELECT CATEGORY FROM NAMES WHERE CURIE = ? LIMIT 1', (results_hash[0][0],))
            results_hash_category = cursor_pref.fetchall()
            if results_hash_category[0][0] in PARAM[col]['expected_classes']:
                logging.info(f'{DB_HASH}\t{val} BECAME {results_hash[0][0]}\tCLASSED\tHASHED\tFIRST TAKEN')
                return results_hash[0][0]
        logging.warning(f'{DB_RES}\t{val} FAILED TO MAP')
        return None
    except Exception as e:
        logging.critical(f'{DB_HASH}\t{val} BROKE CLASSED MAPPING : {e}')
        return None

# Main function to process the DataFrame
def master():
    with open(CONFIG_FILE, 'r') as file:
        CONFIG = yaml.safe_load(file)
    PARAM = CONFIG['params']
    SOURCE = openIt(PARAM)[0]
    if PARAM.get('add_header'):
        SOURCE = SOURCE.rename(PARAM['add_header'])
    if PARAM.get('reindex'):
        SOURCE = reindexIt(SOURCE, PARAM)
    if PARAM.get('ascii'):
        SOURCE = ascii(SOURCE)
    if PARAM.get('attributes'):
        SOURCE = attributes(SOURCE, PARAM)
    if PARAM.get('node_column_format'):
        for col in PARAM['node_column_format']:
            SOURCE = nodeColumnFormat(SOURCE, PARAM, col)
    if PARAM.get('mapping'):
        for col in PARAM['mapping']:
            if PARAM['mapping'][col].get('no_class'):
                SOURCE = SOURCE.with_columns(pl.col(col).apply(classlessDBResHash))
            else:
                SOURCE = SOURCE.with_columns(pl.col(col).apply(lambda val: classedDBResHash(val, PARAM, col)))
    if PARAM.get('save_path'):
        SOURCE.write_csv(PARAM['save_path'])

if __name__ == "__main__":
    master()
