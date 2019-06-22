import re
import os
import sys
import numpy as np
import random
import glob
import click
import paramiko
import time
from concurrent.futures import ThreadPoolExecutor

import logging

logging.basicConfig(
    level   =   logging.INFO,
    format  =   '%(asctime)s %(levelname)s %(message)s',
)

TPCH_T_NAME = {
    'l': 'lineitem',
    'c': 'customer',
    'o': 'orders',
    's': 'supplier',
    'r': 'region',
    'p': 'part',
    'n': 'nation',
    'ps': 'partsupp',
}

PAR_DIR = '/home/ec2-user/tpch_parquet'
INFO_DIR = '/home/ec2-user/info'

ORIGIN_LOC_FILE = 'origin-locs.txt'
REPL_LOC_FILE = 'replica-locs.txt'
IP_NAME_FILE = 'ip-name.txt'
QUERY_COL_FILE = 'queries-tpch.txt'

ALLUXIO_DIR = os.path.dirname(os.path.abspath(__file__))

name_ip_dict = {}
origin_locs = {}
replica_locs = {}

replica_file_path = '{}/{}'.format(ALLUXIO_DIR, REPL_LOC_FILE)

class ReplicaLoc:
    def __init__(self, replica, loc, origin, pairs):
        self.replica = replica
        self.loc = loc
        self.origin = origin
        self.pairs = pairs
    
    def hasPair(self, pair):
        return any([ p[0] == pair[0] and p[1] == pair[1] for p in self.pairs ])

def init_from_file():
    # init node_name->ip dict
    with open('{}/{}'.format(ALLUXIO_DIR, IP_NAME_FILE), 'r') as f:
        for line in f:
            ip_name = [ i for i in line.strip().split(',') if i ]
            name_ip_dict[ip_name[1]] = ip_name[0]

    # init origin locs
    with open('{}/{}'.format(ALLUXIO_DIR, ORIGIN_LOC_FILE), 'r') as f:
        for line in f:
            path_loc = [ i for i in line.strip().split(',') if i ]
            origin_locs[path_loc[0]] = name_ip_dict[path_loc[1]]

    # init replica locs
    if os.path.isfile(replica_file_path):
        with open(replica_file_path, 'r') as f:
            for line in f:
                info = [ i for i in line.strip().split(',') if i ]
                repl_loc = ReplicaLoc(info[0], name_ip_dict[info[1]], info[2], [ (i.split(':')[0], i.split(':')[1]) for i in info[3:] ] )
                replica_locs[info[2]] = replica_locs[info[2]] + [repl_loc] if info[2] in replica_locs else [repl_loc]

# finish init from txt

class ParColumn:
    def __init__(self, col):
        self.col = col
        table_name = TPCH_T_NAME[col.split('_')[0]]
        self.table = table_name
        self.pathes = [ p for p in glob.glob('{}/{}.parquet/*'.format(PAR_DIR, table_name)) if not p.split('/')[-1].startswith('_SUCCESS')] 

        self.path_off_dict = {}
        for path in self.pathes:
            file_name = os.path.basename(path)
            with open('{}/{}.parquet/{}.txt'.format(INFO_DIR, table_name, file_name), 'r') as f:
                for line in f:
                    col_info = line.strip().split(',')
                    if len(col_info) >=3 and col_info[2] == col:
                        self.path_off_dict[path] = (col_info[0], col_info[1])
                        break
        
class ColLocation:
    def __init__(self, path, par_col):
        self.path = path
        self.col = par_col.col
        self.locs = origin_locs[path] # origin table loc

        self.pair = par_col.path_off_dict[path]
        
        self.replicas = set()
        if os.path.isfile(replica_file_path):
            self.replicas = set([ repl_loc.loc for repl_loc in replica_locs[self.path] if repl_loc.hasPair(pair) ])

def parse_all_queries():
    tpch_queries = []
    with open('{}/{}'.format(ALLUXIO_DIR, QUERY_COL_FILE), 'r') as f:
        for line in f:
            cols = [ i for i in line.strip().split(',') if i ]
            tpch_queries.append(cols)

    return tpch_queries

EXE_CMD = 'cd /home/ec2-user/alluxio; java -jar readparquet/target/readparquet-2.0.0-SNAPSHOT.jar'

def send_to_worker(addr, path, cols):
    col_pair_str = ''
    for off, length in cols:
        col_pair_str = col_pair_str + '{} {} '.format(off, length)

    ssh = paramiko.client.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
    ssh.connect(addr, username='ec2-user')

    _, stdout, stderr = ssh.exec_command('{} {} {}'.format(EXE_CMD, path, col_pair_str))
    is_success = stdout.channel.recv_exit_status() == 0
    if not is_success:

        for line in stdout.xreadlines():
            print(line)

now = lambda: time.time()
gap_time = lambda past_time : int((now() - past_time) * 1000)

# the logic in scheduler should avoid offset & len, use column instead
@click.command()
@click.argument('query', type=int)
def submit_query(query):
    all_queries = parse_all_queries()
    if query < 1 or query > len(all_queries):
        print('Invalid query')
        return
    
    init_from_file()
    
    col_to_read = all_queries[query - 1]
    logging.info('Receive query: {}, cols: {}'.format(query, col_to_read))

    all_par_cols = [ ParColumn(c) for c in col_to_read ]
    
    table_set = set([ c.table for c in all_par_cols])
    table_col_dict = { t: [ c for c in all_par_cols if c.table == t ] for t in table_set }

    col_locs_dict = { c: { p: ColLocation(p, c) for p in c.pathes } for c in all_par_cols }

    sched_res = bundling_policy(table_col_dict, col_locs_dict)

    logging.info('Got scheduling result')    
    start = now()

    pool = ThreadPoolExecutor(max_workers=len(sched_res.items()) + 3)
    for p, res in sched_res.items():
        pool.submit(send_to_worker, res[0], p, res[1])
    pool.shutdown(wait=True)

    duration = gap_time(start)
    logging.info('Finish reading. elapsed: {}'.format(duration))


def bundling_policy(table_col_dict, col_locs_dict):
    sched_res = {}
    for t, cols in table_col_dict.items():
        part_files = cols[0].pathes
        for p in part_files:
            avail_locs = [col_locs_dict[c][p] for c in cols ]

            all_cols_repl = avail_locs[0].replicas
            for col_repl in [ l.replicas for l in avail_locs ]:
                all_cols_repl = all_cols_repl.intersection(col_repl)

            col_pair = [ c.path_off_dict[p] for c in cols ]
            if len(all_cols_repl) > 0:
                # random pick one replica to serve
                sched_res[p] = (all_cols_repl[random.randint(0, len(all_cols_repl) - 1)], col_pair)
            else:
                # served by origin table
                sched_res[p] = (origin_locs[p], col_pair)
    
    return sched_res

    
if __name__ == '__main__':
    submit_query()