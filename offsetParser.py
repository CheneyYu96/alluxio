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

    infer_offs = []
    infer_lens = []
    start_i = 4
    for i in range(len(offset_list)):
        if offset_list[i] > start_i:
            infer_offs.append(start_i)
            infer_lens.append(offset_list[i] + length_list[i] - start_i)
        elif offset_list[i] == start_i:
            infer_offs.append(offset_list[i])
            infer_lens.append(length_list[i])
        else:
            print(f'Warn: offset < start index')
        start_i = offset_list[i] + length_list[i]
        
    with open(outpath, 'w') as f:
        for i in range(len(infer_offs)):
            f.write(str(infer_offs[i]) + ',' + str(infer_lens[i])+'\n')

@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def translate(path):
    offset_list = []
    length_list = []
    with open(path, 'r') as f:
        for line in f:
            line_list = line.split(',')
            offset_list.append(int(line_list[0]))
            length_list.append(int(line_list[1]))
    
    infer_offs = []
    infer_lens = []
    start_i = 4
    for i in range(len(offset_list)):
        if offset_list[i] > start_i:
            infer_offs.append(start_i)
            infer_lens.append(offset_list[i] + length_list[i] - start_i)
        elif offset_list[i] == start_i:
            infer_offs.append(offset_list[i])
            infer_lens.append(length_list[i])
        else:
            print(f'Warn: offset < start index')
        start_i = offset_list[i] + length_list[i]

    pair = list(zip(infer_offs, infer_lens))
    for o, l in pair:
        print(f'{l:12} {o:12}')

if __name__ == '__main__':
    parse()
    # translate()