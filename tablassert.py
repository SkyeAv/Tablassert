''' Skye Goetz (ISB) 09/11/2024 '''

import os
import sys
import copy
import math
import yaml # Download This
import sqlite3 # Download This
import logging
import subprocess
import polars as pl # Download This
from urllib.request import urlretrieve

BIN = os.environ['tablassert']

try:
    CONFIG_FILE = sys.argv[1]
    SOURCE_DATA = sys.argv[2] # Source Data Folder Path (The One Master Creates)
except IndexError:
    print(f'Usage : python3 {os.path.basename(sys.argv[0])} <supplementalTableConfig.yml> <Source Data Path>')
    sys.exit()

# Sets Up the Log File/Folder
LOG_PATH = os.path.join(SOURCE_DATA, 'log')
os.makedirs(LOG_PATH, exist_ok=True)
if os.path.isfile(os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log')):
    os.remove(os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log'))
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s\t%(levelname)s\t%(message)s',
    filename= os.path.join(LOG_PATH, f'{os.path.basename(CONFIG_FILE)}.log'), filemode='w'
)

# Connects and Configures SQLite DBs for Optimal Mapping
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

# Downloads File, Checks Extension, Converts it to a DataFrame
def openIt(PARAM):
    os.makedirs(os.path.dirname(PARAM['data_location']['path_to_file']), exist_ok=True)
    if not os.path.isfile(PARAM['data_location']['path_to_file']):
        urlretrieve(PARAM['provenance']['table_url'], PARAM['data_location']['path_to_file'])
    EXT = os.path.splitext(os.path.basename(PARAM['data_location']['path_to_file']))[1]
    if EXT in ['.xls', '.xlsx', '.XLS', '.XLSX']:
        source = pl.read_excel(PARAM['data_location']['path_to_file'], sheet_name=PARAM['data_location']['sheet_to_use'], has_header=False)
        source = source.slice((int(PARAM['data_location']['first_line']) - 1), PARAM['data_location']['last_line'] - 1) # So Unintended Rows Neither Exist Nor Map
    elif EXT in ['.csv', '.tsv', 'txt']:
        source = pl.read_csv(PARAM['data_location']['path_to_file'], separator=PARAM['data_location']['delimiter'])
    else:
        print(f'Sorry. Tablassert doesn\'t yet support {EXT} files.')
    return source, EXT

# Creates Columns Under the Attributes Section of the YAML
def attributes(source, PARAM):
    for attribute, config in PARAM['attributes'].items():
        if attribute in source.columns and config.get('column_name') != attribute: # Deletes Columns With The Same Name As Final KG When They're Unessecary
            source = source.drop(attribute)
        if 'value' in config:
            source = source.with_columns(pl.lit(config['value']).alias(attribute))
        else:
            source = source.rename({config['column_name']: attribute})
        if 'math' in config: # This Uses The Math Module For All Transformations
            for op in config['math']:
                operation = getattr(math, op['operation']) # Hence getattr
                val = float(op.get('parameter', '0'))
                order = bool(op.get('order_last'))
                if order:
                    # We have to do this list thing b/c our machine forced us to use a version of polars w/o map_elements : (
                    values = [operation(float(x), val) if x is not None else None for x in source[attribute].to_list()]
                    source = source.with_columns(pl.Series(attribute, values).alias(attribute))
                else:
                    values = [operation(val, float(x)) if x is not None else None for x in source[attribute].to_list()]
                    source = source.with_columns(pl.Series(attribute, values).alias(attribute))
    return source

# Get's the A-ZZ Equivalent to Each Column
def getXlsxColumnName(n):
    if n < 26:
        return chr(n + 65)
    else:
        first_letter = chr((n - 26) // 26 + 65)
        second_letter = chr((n % 26) + 65)
        return first_letter + second_letter

# Renames Each DataFrame Column to its A-ZZ equivalent if Table is an Excel File
def ascii(source):
    new_column_names = [getXlsxColumnName(i) for i in range(len(source.columns))]
    source = source.rename({old_name: new_name for old_name, new_name in zip(source.columns, new_column_names)})
    return source

# This is the Function Behind the reindex Section in the YAML
def reindexIt(source, PARAM):
    for op in PARAM['reindex']:
        val = float(
            op['value']) if isinstance(op['value'], (int, float)) \
            else str(op['value']
        ) # if_equals Allows Strings so Users Can Remove A Certain Sting Value From Output
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

# This Formats Everything in any Subject/Object Column
def nodeColumnFormat(source, PARAM, col):
    if 'fill_values' in PARAM[col]:
        source = source.with_columns(pl.col(col).fill_null(strategy=PARAM[col]['fill_values']))
    if 'explode_column' in PARAM[col]: # This also str splits the column so there's a list b4 exploding
        source = source.with_columns(pl.col(col).str.split(PARAM[col]['explode_column'])).explode(col)
    if 'regex_replacements' in PARAM[col]:
        for replacement in PARAM[col]['regex_replacements']:
            repl = replacement['replacement'] if replacement['replacement'] is not None else '' # This deals with the ~s
            source = source.with_columns(
                pl.col(col).str.replace_all(replacement['pattern'], repl).alias(col)
            )
    if 'prefix' in PARAM[col]:
        for prefix in PARAM[col]['prefix']:
            source = source.with_columns(
                (pl.lit(prefix['prefix']) + pl.col(col).cast(pl.Utf8)).alias(col)
            )
    return source

# This is the Classless Mapping Functions
def classlessDBResHash(val):
    try:
        cursor_res = conn_res.cursor()
        cursor_res.execute('SELECT CURIE FROM SYNONYMS WHERE SYNONYM = ? LIMIT 1', (val,)) # b/c of the current indexing subqueries are WAY TOO SLOW
        results_res = cursor_res.fetchall()
        if results_res: # This if ladder is WAY FASTER
            logging.info(f'{DB_RES}\t{val} BECAME {results_res[0][0]}\tCLASSLESS\tFIRST TAKEN')
            return results_res[0][0] # the [0][0] undo the list inside a tuple that SQLite returns for some reason
        if not os.path.isfile(os.path.join(BIN, 'hashingAndRegex')):
            os.system(f'g++ -std=c++11 -o {BIN}/hashingAndRegex {BIN}/hashingAndRegex.cpp -lssl -lcrypto') # This could go wrong if people dont have g++
        result = subprocess.run(os.path.join(BIN, 'hashingAndRegex'), input=val, text=True, capture_output=True, check=True)
        hashed_val = result.stdout.strip()
        cursor_hash = conn_hash.cursor()
        cursor_hash.execute('SELECT CURIE FROM HASHES WHERE HASH = ? LIMIT 1', (hashed_val,)) # Same Indexing Problem
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

# Does classed mapping and passes to classless if it fails
def classedDBResHash(val, PARAM, col):
    try:
        cursor_res = conn_res.cursor()
        cursor_res.execute('SELECT CURIE FROM SYNONYMS WHERE SYNONYM = ? LIMIT 1', (val,)) # Same Indexing Problem
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
        cursor_hash.execute('SELECT CURIE FROM HASHES WHERE HASH = ? LIMIT 1', (hashed_val,)) # Same Indexing Problem
        results_hash = cursor_hash.fetchall()
        if results_hash:
            cursor_pref = conn_pref.cursor()
            cursor_pref.execute('SELECT CATEGORY FROM NAMES WHERE CURIE = ? LIMIT 1', (results_hash[0][0],))
            results_hash_category = cursor_pref.fetchall()
            if results_hash_category[0][0] in PARAM[col]['expected_classes']:
                logging.info(f'{DB_HASH}\t{val} BECAME {results_hash[0][0]}\tCLASSED\tFIRST TAKEN')
                return results_hash[0][0]
            else: 
                return classlessDBResHash(val)
        else:
            return classlessDBResHash(val)
    except Exception as e:
        logging.critical(f'{DB_HASH}\t{val} BROKE CLASSED MAPPING : {e}')
        return classlessDBResHash(val)

# Does Mapping
def DBMap(val):
    if not val: 
        return None
    try:
        cursor_map = conn_map.cursor()
        cursor_map.execute('SELECT PREFERRED FROM MAP WHERE ALIAS = ? LIMIT 1', (val,))
        results_map = cursor_map.fetchall()
        if results_map:
            logging.info(f'{DB_MAP}\t{val} BECAME {results_map[0][0]}')
            return results_map[0][0]
        else:
            logging.info(f'{DB_MAP}\t{val} REMAINED {val}')
            return val
    except Exception as e: 
        logging.critical(f'{DB_MAP}\t{val} BROKE DB MAP : {e}')
        return val

# Finds preferred name
def DBPrefName(val):
    if not val: 
        return None
    try:
        cursor_pref = conn_pref.cursor()
        cursor_pref.execute('SELECT NAME FROM NAMES WHERE CURIE = ? LIMIT 1', (val,))
        results_pref = cursor_pref.fetchall()
        if results_pref:
            logging.info(f'{DB_PREF} NAME\t{val} BECAME {results_pref[0][0]}')
            return results_pref[0][0]
        else:
            logging.warning(f'{DB_PREF} NAME\t{val} FAILED TO MAP')
            return None
    except Exception as e: 
        logging.critical(f'{DB_PREF}\t{val} BROKE DB PREF NAME : {e}')
        return None

# Finds category but is separate from name b/c of slow indexing again
def DBPrefCategory(val):
    if not val: 
        return None
    try:
        cursor_pref = conn_pref.cursor()
        cursor_pref.execute('SELECT CATEGORY FROM NAMES WHERE CURIE = ? LIMIT 1', (val,))
        results_pref = cursor_pref.fetchall()
        if results_pref:
            logging.info(f'{DB_PREF} CATEGORY\t{val} BECAME {results_pref[0][0]}')
            return 'biolink:' + results_pref[0][0]
        else:
            logging.warning(f'{DB_PREF} CATEGORY\t{val} FAILED TO MAP')
            return None
    except Exception as e: 
        logging.critical(f'{DB_PREF}\t{val} BROKE DB PREF CATEGORY : {e}')
        return None

# This is like main but for Node columns (Subject/Object) b/c the script does the most to them
def nodeObjects(source, PARAM):
    for column in ['subject', 'object']:
        if 'curie' in PARAM[column] or 'value' in PARAM[column]:
            source = source.with_columns(
                pl.lit(PARAM[column].get('curie', PARAM[column].get('value'))).alias(column)
            )
        elif 'curie_column_name' in PARAM[column] or 'value_column_name' in PARAM[column]:
            source = source.rename(
                {PARAM[column].get('curie_column_name', PARAM[column].get('value_column_name')): column}
            )
        source = nodeColumnFormat(source, PARAM, column)
        if 'value' in PARAM[column] or 'value_column_name' in PARAM[column]:
            if not 'expected_classes' in PARAM[column]: 
            # We have to do this list thing b/c our machine forced us to use a version of polars w/o map_elements : (
                res_has = [classlessDBResHash(x) if x is not None else None for x in source[column].to_list()]
            else:
                res_has = [classedDBResHash(x, PARAM, column) if x is not None else None for x in source[column].to_list()]
            source = source.with_columns(pl.Series(column, res_has).alias(column))
        mapping = [DBMap(x) if x is not None else None for x in source[column].to_list()]
        source = source.with_columns(pl.Series(column, mapping).alias(column))
        names = [DBPrefName(x) if x is not None else None for x in source[column].to_list()]
        source = source.with_columns(pl.Series(column, names).alias(f'{column}_name'))
        categories = [DBPrefCategory(x) if x is not None else None for x in source[column].to_list()]
        source = source.with_columns(pl.Series(column, categories).alias(f'{column}_category'))
    conn_res.close() # This saves overhead
    conn_hash.close()
    conn_map.close()
    conn_pref.close()
    return source

# This does all the main transformations
def main(PARAM):
    source, EXT = openIt(PARAM)
    if EXT in ['.xls', '.xlsx', '.XLS', '.XLSX']:
        source = ascii(source)
    for provenance, val in PARAM['provenance'].items(): 
        source = source.with_columns(pl.lit(val).alias(provenance))
    sheet_to_use = PARAM['data_location']['sheet_to_use'] if EXT in ['.xls', '.xlsx', '.XLS', '.XLSX'] else 'not_applicable'
    source = source.with_columns(pl.lit(sheet_to_use).alias('sheet_name'))
    source = source.with_columns(pl.lit(PARAM['predicate']).alias('predicate'))
    source = attributes(source, PARAM)
    if 'reindex' in PARAM: 
        source = reindexIt(source, PARAM)
    source = nodeObjects(source, PARAM)
    source = source.select([
            'subject', 'predicate', 'object', 'subject_name', 'object_name', 'n', 'relationship_strength', 
            'p', 'relationship_type', 'p_correction_method', 'knowledge_level', 'agent_type', 
            'publication', 'publication_name', 'author_year', 'table_url', 'sheet_name', 
            'yaml_curator_and_organization', 'subject_category', 'object_category'
    ]).unique().drop_nulls() # These are kinda hardcoded thoughout the app
    INTERMEDIATE_PATH = os.path.join(SOURCE_DATA, f'{os.path.basename(CONFIG_FILE)}.tsv')
    if not os.path.isfile(INTERMEDIATE_PATH):
        source = source.with_columns([pl.col(col).cast(pl.Utf8) for col in source.columns]) # We have to cast everything as a string to dtypes match for polars
        source.write_csv(INTERMEDIATE_PATH, separator='\t')
    else:
        source = source.with_columns([pl.col(col).cast(pl.Utf8) for col in source.columns])
        intermediate = pl.read_csv(INTERMEDIATE_PATH, separator='\t', schema_overrides={'p': pl.Utf8, 'relationship_strength': pl.Utf8}, ignore_errors=True).drop_nulls()
        intermediate = intermediate.with_columns([pl.col(col).cast(pl.Utf8) for col in intermediate.columns])
        source = pl.concat([source, intermediate]).unique()
        source.write_csv(INTERMEDIATE_PATH, separator='\t')

# This is the master which executes main but basically just serves the sections feature
def master():
    with open(CONFIG_FILE) as temp:
        CONFIG = yaml.load(temp, Loader=yaml.FullLoader)
    if 'sections' in CONFIG:
        for section in CONFIG['sections']:
            temp_config = copy.deepcopy(CONFIG)
            for key, value in section.items():
                if key in temp_config: 
                    # The if ladder deals with all the possible updates people could make in sections so they have to rewirte as litle as possible in each section
                    if isinstance(temp_config[key], list) and isinstance(value, list):
                        temp_config[key].extend(value)
                    elif isinstance(temp_config[key], dict) and isinstance(value, dict):
                        temp_config[key].update(value)
                    else:
                        temp_config[key] = value
                else:
                    temp_config[key] = value
            main(temp_config)
    else:
        main(CONFIG)

master()