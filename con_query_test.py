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

class ZipFGenerator:
	def __init__(self, count, alpha):
		self.ps = [1.0 / (i + 1) ** alpha for i in range(count)]
		s = sum(self.ps)
		self.ps = map(lambda x: x / s, self.ps)

	def generate(self, size):
		words = size
		num_per_word = map(lambda x: x * words, self.ps)

		res = []
		for i, n in enumerate(num_per_word):
			c = int(n) if int(n) > 0 else 1
			res.append(c)

		return res

@click.command()
@click.argument('rate', type=int) # number of query per minute
@click.argument('timeout', type=int) # minute
@click.argument('query', type=int)
@click.argument('logdir', type=str)
@click.option('--policy', type=int, default=0) # 0: bundling, 1: column-wise, 2: table
@click.option('--fault', type=int, default=0)
@click.option('--gt', type=bool, default=True)
def poisson_test(rate, timeout, query, logdir, policy, fault, gt):

    metrics = {}

    pool = ThreadPoolExecutor(rate * 3)
    for _ in range(timeout):
        avg_interval = 60 * 1.0 / rate
        interval_samples = np.random.poisson(avg_interval, rate)

        for s in interval_samples:
            act_q = -1
            if query == 0:
                q_p_list = list(query_pop_dict.items())
                q_p_list.sort(key=lambda e: e[0])

                p_sum = sum([ p for _, p in q_p_list])
                prob = [ p * 1.0/ p_sum for _, p in q_p_list ]

                q_index = np.random.choice(len(q_p_list), 1, p=prob)[0]
                act_q = q_p_list[q_index][0]

            else:
                act_q = query
            
            metrics[act_q] = metrics[act_q] + 1 if act_q in metrics else 1
            logging.info('Generate query: {}, sleep: {}'.format(act_q, s))

            sub_dir = '{}/q{}/c{}'.format(logdir, act_q, metrics[act_q])
            pool.submit(send_query, act_q, sub_dir, policy, fault, gt)
            time.sleep(s)

    sorted_metrics = list(metrics.items())
    sorted_metrics.sort(key=lambda e: e[0])
    for q, c in sorted_metrics:
        print('query={} , count={}'.format(q, c))

def send_query(query, sub_dir, policy, fault, gt):
    mkdir(sub_dir)
    os.system('python query_scheduler.py {} {} --policy {} --fault {} --gt {} > {}/master.log 2>&1'
        .format(query, sub_dir, policy, fault, gt, sub_dir))

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
    # g = ZipFGenerator(22, 1.)
    # pops = g.generate(size=100)

    # with open(pop_file, 'w') as f:
    #     for i in range(len(pops)):
    #         f.write(str(i+1) + ',' + str(pops[i])+ '\n')


