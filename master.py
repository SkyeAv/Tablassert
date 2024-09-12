''' Skye Goetz (ISB) 09/11/2024 '''

import os
import sys
import yaml
import glob
import logging
import subprocess
import polars as pl
from concurrent.futures import ProcessPoolExecutor

try: 
    GRAPH_INFO = sys.argv[1]
except IndexError:
    print(f'Usage : python3 {os.path.basename(sys.argv[0])} <knowledgeGraphInfo.yml>')
    sys.exit()

os.environ['tablassert'] = os.path.dirname(os.path.abspath(sys.argv[0]))
BIN = os.environ['tablassert']

with open(GRAPH_INFO) as config:
    PARAM = yaml.load(config, Loader=yaml.FullLoader)
SOURCE_DATA = os.path.join(os.getcwd(), f'{PARAM['knowledge_graph_name']}.source.data')
LOG_PATH = os.path.join(os.getcwd(), f'{PARAM['knowledge_graph_name']}.log')

if os.path.isfile(LOG_PATH): os.remove(LOG_PATH)

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s\t%(levelname)s\t%(message)s',
    filename= LOG_PATH, filemode='w'
)

logging.info('master.py\tSTARTED')

os.environ['tablassertDBMap'] = PARAM['DB_map']
os.environ['tablassertDBRes'] = PARAM['DB_res']
os.environ['tablassertDBPref'] = PARAM['DB_pref']
os.environ['tablassertDBHash'] = PARAM['DB_hash']

os.environ['tablassertNiceValue'] = str(PARAM['re_nice'])
os.nice(int(os.environ['tablassertNiceValue']))

def callWorker(YAML):
    logging.info(f'{YAML}\tSTARTED')
    try: 
        subprocess.run(['python3', os.path.join(BIN, 'tablassert.py'), YAML, SOURCE_DATA], check=True)
        logging.info(f'{YAML}\tCOMPLETED')
    except subprocess.CalledProcessError as e: 
        logging.info(f'{YAML}\tFAILED : {e}')
        pass

def safeConversion(val):
    try: 
        return float(val)
    except ValueError: 
        return None

def pvalCutoff(source):
    floated_vals = [safeConversion(x) if x is not None else None for x in source['p'].to_list()]
    source = source.with_columns(pl.Series('p', floated_vals).alias('p_float'))
    mask_valid = pl.col('p_float').is_not_null() & (pl.col('p_float') <= float(PARAM['p_value_cutoff']))
    mask_invalid = pl.col('p_float').is_null()
    source = source.filter(mask_valid | mask_invalid)
    source = source.drop('p_float')
    return source

def master():
    os.makedirs(SOURCE_DATA, exist_ok=True)
    with ProcessPoolExecutor(max_workers=PARAM['max_workers']) as executor:
        yamls = []
        for dir in PARAM['config_directories']:
            yamls.extend(glob.glob(os.path.join(dir, '*.yml')))
        for yaml in yamls:
            if not os.path.isfile(os.path.join(SOURCE_DATA, f'{os.path.basename(yaml)}.tsv')):
                executor.submit(callWorker, yaml)
            else:
                logging.info(f'{yaml}\tSKIPPED')
    EDGES_PATH = os.path.join(os.getcwd(), f'{PARAM['knowledge_graph_name']}.edges.tsv')
    NODES_PATH = os.path.join(os.getcwd(), f'{PARAM['knowledge_graph_name']}.nodes.tsv')
    if os.path.isfile(EDGES_PATH):
        os.remove(EDGES_PATH)
    if os.path.isfile(NODES_PATH):
        os.remove(NODES_PATH)
    for intermediate in glob.glob(os.path.join(SOURCE_DATA, '*.tsv')):
        source = pl.read_csv(intermediate, separator='\t', schema_overrides={'p': pl.Utf8, 'relationship_strength': pl.Utf8}, ignore_errors=True).pipe(pvalCutoff)
        source = source.with_columns([pl.col(col).cast(pl.Utf8) for col in source.columns])
        if not os.path.isfile(EDGES_PATH):
            source.write_csv(EDGES_PATH, separator='\t')
        else:
            intermediate_edges = pl.read_csv(EDGES_PATH, separator='\t', schema_overrides={'p': pl.Utf8, 'relationship_strength': pl.Utf8}, ignore_errors=True).drop_nulls()
            intermediate_edges = intermediate_edges.with_columns([pl.col(col).cast(pl.Utf8) for col in intermediate_edges.columns])
            edges = pl.concat([source, intermediate_edges]).unique()
            edges.write_csv(EDGES_PATH, separator='\t')
        logging.info(f'{os.path.basename(intermediate)}\tAPPENDED TO FINAL TSVs')
    intermediate_source = pl.read_csv(EDGES_PATH, separator='\t', schema_overrides={'p': pl.Utf8, 'relationship_strength': pl.Utf8}, ignore_errors=True).drop_nulls()
    logging.info(f'{os.path.basename(NODES_PATH)}\tSTARTED PROCESSING')
    subject_nodes = intermediate_source.select([
        pl.col('subject').alias('id'), pl.col('subject_name').alias('name'), pl.col('subject_category').alias('category')
    ])
    object_nodes = intermediate_source.select([
        pl.col('object').alias('id'), pl.col('object_name').alias('name'), pl.col('object_category').alias('category')
    ])
    nodes = pl.concat([subject_nodes, object_nodes]).unique()
    nodes.write_csv(NODES_PATH, separator='\t')
    logging.info(f'{os.path.basename(NODES_PATH)}\tCOMPLETED')
    logging.info(f'{os.path.basename(EDGES_PATH)}\tSTARTED PROCESSING')
    edges = intermediate_source.select([
        'subject', 'predicate', 'object', 'subject_name', 'object_name', 'n', 'relationship_strength', 
        'p', 'relationship_type', 'p_correction_method', 'knowledge_level', 'agent_type', 'publication',
        'publication_name', 'author_year', 'table_url', 'sheet_name', 'yaml_curator_and_organization'
    ])
    if not PARAM['ploverize_graph']:
        edges.write_csv(EDGES_PATH, separator='\t')
    else: 
        edges.drop(['subject_name', 'object_name'])
        edges.write_csv(EDGES_PATH, separator='\t')
        GZ_NODES_PATH = os.path.join(os.path.dirname(NODES_PATH), f'{os.path.basename(NODES_PATH)}.gz')
        GZ_EDGES_PATH = os.path.join(os.path.dirname(NODES_PATH), f'{os.path.basename(EDGES_PATH)}.gz')
        if os.path.isfile(GZ_NODES_PATH):
            os.remove(GZ_NODES_PATH)
        if os.path.isfile(GZ_EDGES_PATH):
            os.remove(GZ_EDGES_PATH)
        os.system(f'gzip {NODES_PATH}')
        os.system(f'gzip {EDGES_PATH}')
    logging.info(f'{os.path.basename(EDGES_PATH)}\tCOMPLETED')
    
if __name__ == "__main__":
    master()
    logging.info('master.py\tCOMPLETE')