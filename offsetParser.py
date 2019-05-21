import re
import os
import sys
import click

@click.command()
@click.argument('inpath', type=click.Path(exists=True, resolve_path=True))
@click.argument('outpath', type=click.Path(exists=True, resolve_path=True))
def parse(inpath, outpath):
    index = 0
    offset_list = []
    length_list = []

    with open(inpath, 'r') as f:
        flag = False
        for line in f:
            line_list = line.split(' ')
            line_list = [x for x in line_list if x]
            if (re.match('column', line_list[0])):
                flag = False
            elif (re.match('offset', line_list[0])):
                flag = True
            if (flag and re.match('page',line_list[0])):
                index +=1
                if (index % 2==0):
                    offset_list.append(line_list[1])
                    length_list.append(line_list[2])


    with open(outpath, 'w') as f:
        for i in range(len(offset_list)):
            f.write(offset_list[i]+','+length_list[i]+'\n')

if __name__ == '__main__':
    parse()