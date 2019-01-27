import glob
import re
import click
import math

def extract_info(path):
    files_path = glob.glob('{}/workerLoad*'.format(path))
    records = {}
    for p in files_path:
        with open(p) as f:
            for line in f:
                content = line.split('\t')
                # second
                timestamp = int(int(content[0])/1000)
                # KB
                amount = int(content[2])/(1024 * 1024)
                records[timestamp] = (records[timestamp] + amount) if timestamp in records else amount

    if records:
        amount_list = list(records.values())
        max_rate = max(amount_list)
        total_shuffling = sum(amount_list)
        # records.sort(key=lambda e: e[0])

        print('max_rate: {}; shuffle: {}'.format(max_rate, total_shuffling))
    else:
        print('No records')

@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
# @click.option('--num', default=1)
def parse(path):
    extract_info(path)

if __name__ == '__main__':
    parse()
