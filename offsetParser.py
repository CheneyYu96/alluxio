#!/usr/local/bin/python3

import re
import os
import sys
from pathlib import Path

file = sys.argv[1]

f = open(file, 'r')

index = 0
offset_list = []
length_list = []
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

f.close()

# dir_name = os.path.dirname(file)
dir_name = Path(file).resolve().parents[0]

# fo = open(dir_name + "/offset.txt", 'w')
fo = open(dir_name / "offset.txt", 'w')
for i in range(len(offset_list)):
    fo.write(offset_list[i]+','+length_list[i]+'\n')

fo.close()