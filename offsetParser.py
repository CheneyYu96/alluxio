import re
import os
import sys
import click

@click.command()
@click.argument('inpath', type=click.Path(exists=True, resolve_path=True))
@click.argument('outpath', type=str)
def parse(inpath, outpath):
    index = 0
    offset_list = []
    length_list = []

    with open(inpath, 'r') as f:
        flag = False
        offs = []
        lens = []
        for line in f:
            line_list = line.split(' ')
            line_list = [x for x in line_list if x]
            if (re.match('column', line_list[0])):
                flag = False
                if len(offs) > 0:
                    act_off = offs[0]
                    act_len = sum(lens)
                    offset_list.append(act_off)
                    length_list.append(act_len)

                    offs = []
                    lens = []

            elif (re.match('offset', line_list[0])):
                flag = True
            if (flag and re.match('page',line_list[0])):
                offs.append(int(line_list[1]))
                lens.append(int(line_list[2]))

        # last column
        if len(offs) > 0:  
            act_off = offs[0]
            act_len = sum(lens)
            offset_list.append(act_off)
            length_list.append(act_len)
        
    with open(outpath, 'w') as f:
        for i in range(len(offset_list)):
            f.write(str(offset_list[i]) + ',' + str(length_list[i])+'\n')

if __name__ == '__main__':
    parse()