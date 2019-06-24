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

replica_file_path = '{}/../{}'.format(ALLUXIO_DIR, REPL_LOC_FILE)

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

        pair = par_col.path_off_dict[path]
        
        self.replicas = set()
        if self.path in replica_locs:
            self.replicas = set([ repl_loc.loc for repl_loc in replica_locs[self.path] if repl_loc.hasPair(pair) ])

def parse_all_queries():
    tpch_queries = []
    with open('{}/{}'.format(ALLUXIO_DIR, QUERY_COL_FILE), 'r') as f:
        for line in f:
            cols = [ i for i in line.strip().split(',') if i ]
            tpch_queries.append(cols)

    return tpch_queries

def get_unique_log_name(path):
    table_name = path.split('/')[-2].split('.')[0]
    part_name = path.split('/')[-1].split('-')[1]
    timestamp = int(round(now() * 1000))
    return '{}-{}-{}.log'.format(table_name, part_name, timestamp)

LOG_PREFIX = '/home/ec2-user/logs'
EXE_CMD = 'cd /home/ec2-user/alluxio/readparquet; java -jar target/readparquet-2.0.0-SNAPSHOT.jar'

MAX_RETRY = 6

def send_cmd_to_worker(ssh_client, cmd, log_name):
    _, stdout, stderr = ssh_client.exec_command('{} > {}/{} 2>&1'.format(cmd, LOG_PREFIX, log_name))
    is_success = stdout.channel.recv_exit_status() == 0
    if not is_success:
        for line in stdout.xreadlines():
            print(line)

def gen_exe_plan(addr, path, cols, alternatives):
    col_pair_str = ''
    for off, length in cols:
        col_pair_str = col_pair_str + '{} {} '.format(off, length)

    cmd_str = '{} {} {}'.format(EXE_CMD, path, col_pair_str)

    ssh = paramiko.client.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

    count = 0
    selected_ip = addr

    retry = MAX_RETRY
    alter_index = 0
    all_servers = list(set(name_ip_dict.values()) - set(alternatives + [addr]))

    while retry > 0:
        try:
            ssh.connect(hostname=selected_ip, username='ec2-user', timeout=300)
            break
        except:
            failed_ip = selected_ip
            selected_ip = alternatives[alter_index] if alter_index < len(alternatives) else all_servers[random.randint(0, len(all_servers) - 1)]
            logging.warn('Fail to establish connection. ip: {}. next_choice: {}'.format(failed_ip, selected_ip))
            alter_index += 1
            retry -= 1

    if retry == 0:
        logging.warn('Fail connects. attemps: {}'.format(MAX_RETRY))
        exit(1)

    log_name = get_unique_log_name(path)
    log_info[log_name] = ssh

    return (ssh, cmd_str, log_name)

def exe_task(addr, path, cols, alternatives):
    (ssh, cmd_str, log_name) = gen_exe_plan(addr, path, cols, alternatives)
    send_cmd_to_worker(ssh, cmd_str, log_name)


log_info = {}
now = lambda: time.time()
gap_time = lambda past_time : int((now() - past_time) * 1000)

# the logic in scheduler should avoid offset & len, use column instead
@click.command()
@click.argument('query', type=int)
@click.argument('logs-dir', type=click.Path(exists=True, resolve_path=True))
@click.option('--policy', type=int, default=0) # 1: column-wise, 0: bundling
def submit_query(query, logs_dir, policy):
    submit_query_internal(query, logs_dir, policy)

def submit_query_internal(query, logs_dir, policy):
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

    sched_res = policies[policy](table_col_dict, col_locs_dict)

    logging.info('Got schedule result.')
    start = now()

    pool = ThreadPoolExecutor(max_workers=len(sched_res.items()) + 3)

    # exe_plan = [ gen_exe_plan(res[0], p, res[1], res[2]) for p, res in sched_res.items()]
    # for ssh_client, cmd, log_name in exe_plan:
    #     pool.submit(send_cmd_to_worker, ssh_client, cmd, log_name)
    for p, res in sched_res.items():
        pool.submit(exe_task, res[0], p, res[1], res[2])
    pool.shutdown(wait=True)

    logging.info('All reading task finished. elapsed: {}'.format(gap_time(start)))

    # collect worker log
    start = now()

    for log_name, ssh_client in log_info.items():
        sftp = ssh_client.open_sftp()
        sftp.get('{}/{}'.format(LOG_PREFIX, log_name), '{}/{}'.format(logs_dir, log_name))
        ssh_client.close()
    
    logging.info('Finish log collection. elapsed: {}'.format(gap_time(start)))

def bundling_policy(table_col_dict, col_locs_dict):
    sched_res = {}
    for t, cols in table_col_dict.items():
        part_files = cols[0].pathes
        for p in part_files:
            avail_locs = [col_locs_dict[c][p] for c in cols ]

            all_cols_repl = avail_locs[0].replicas
            all_possible_locs = set()
            for col_repl in [ l.replicas for l in avail_locs ]:
                all_cols_repl = all_cols_repl.intersection(col_repl)
                all_possible_locs = all_possible_locs.union(col_repl)

            col_pair = [ c.path_off_dict[p] for c in cols ]

            all_cols_repl = list(all_cols_repl)
            all_possible_locs = list(all_possible_locs)
            if len(all_cols_repl) > 0:
                # random pick one replica to serve
                random.shuffle(all_cols_repl)
                sched_res[p] = (all_cols_repl[0], col_pair, all_cols_repl[1:] + [origin_locs[p]])
            else:
                # served by origin table
                random.shuffle(all_possible_locs)
                sched_res[p] = (origin_locs[p], col_pair, all_possible_locs)
    return sched_res

def col_wise_policy(table_col_dict, col_locs_dict):
    sched_res = {}
    for t, cols in table_col_dict.items():
        part_files = cols[0].pathes
        for p in part_files:
            avail_locs = [col_locs_dict[c][p] for c in cols ]

            all_possible_locs = set([origin_locs[p]])
            for col_repl in [ l.replicas for l in avail_locs ]:
                all_possible_locs = all_possible_locs.union(col_repl)

            col_pair = [ c.path_off_dict[p] for c in cols ]
            # random pick one replica to serve
            all_possible_locs = list(all_possible_locs)
            random.shuffle(all_possible_locs)
            sched_res[p] = (all_possible_locs[0], col_pair, all_possible_locs[1:])
    return sched_res

policies = {
    0: bundling_policy,
    1: col_wise_policy,
}

if __name__ == '__main__':
    submit_query()