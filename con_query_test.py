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
import query_scheduler

import logging
logging.basicConfig(
    level   =   logging.INFO,
    format  =   '%(asctime)s %(levelname)s %(message)s',
)

now = lambda: time.time()
gap_time = lambda past_time : int((now() - past_time) * 1000)

ALLUXIO_DIR = os.path.dirname(os.path.abspath(__file__))
pop_file = '{}/{}'.format(ALLUXIO_DIR, 'pop.txt')

query_pop_dict = {}
if os.path.isfile(pop_file):
    with open(pop_file, 'r') as f:
        for line in f:
            q_p = [ i for i in line.strip().split(',') if i ]
            query_pop_dict[int(q_p[0])] = int(q_p[1])


@click.command()
@click.argument('rate', type=int) # number of query per minute
@click.argument('timeout', type=int) # minute
@click.argument('query', type=int)
@click.argument('logdir', type=str)
@click.option('--policy', type=int, default=0) # 1: column-wise, 0: bundling
def poisson_test(rate, timeout, query, logdir, policy):

    metrics = {}

    pool = ThreadPoolExecutor(rate * 3)
    for _ in range(timeout):
        avg_interval = 60 * 1.0 / rate
        interval_samples = np.random.poisson(avg_interval, rate)

        for s in interval_samples:
            act_q = -1
            if query == 0:
                pass
            else:
                act_q = query
            
            metrics[act_q] = metrics[act_q] + 1 if act_q in metrics else 1
            logging.info('Generate query: {}, sleep: {}'.format(act_q, s))

            sub_dir = '{}/q{}/c{}'.format(logdir, act_q, metrics[act_q])
            pool.submit(send_query, query, sub_dir, policy)
            time.sleep(s)

    for q, c in metrics.items():
        print('query={} , count={}'.format(q, c))

def send_query(query, sub_dir, policy):
    mkdir(sub_dir)
    os.system('python query_scheduler.py {} {} --policy {} > {}/master.log'.format(query, sub_dir, policy, sub_dir))
    # query_scheduler.submit_query_internal(query, sub_dir, policy)

def mkdir(newdir):
    if type(newdir) is not str:
        newdir = str(newdir)
    if os.path.isdir(newdir):
        pass
    elif os.path.isfile(newdir):
        raise OSError("a file with the same name as the desired " \
                      "dir, '%s', already exists." % newdir)
    else:
        head, tail = os.path.split(newdir)
        if head and not os.path.isdir(head):
            mkdir(head)
        if tail:
            os.mkdir(newdir)

if __name__ == '__main__':
    poisson_test()