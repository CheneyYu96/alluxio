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

now = lambda: time.time()
gap_time = lambda past_time : int((now() - past_time) * 1000)

@click.command()
@click.argument('rate', type=int) # number of query per minute
@click.argument('timeout', type=int) # minute
@click.argument('logdir', type=str)
def submit_query(rate, timeout, logdir):
    pool = ThreadPoolExecutor(50)
    for i in range(timeout):


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