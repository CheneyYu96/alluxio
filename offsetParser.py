#!/usr/local/bin/python3

import re

f = open('output.txt', 'r')

index = 0
offset_list = []
length_list = []
for line in f:
    line_list = line.split(' ')
    line_list = [x for x in line_list if x]
    if (re.match('page',line_list[0])):
        index +=1
        if (index % 2==0):
            offset_list.append(line_list[1])
            length_list.append(line_list[2])

f.close()

fo = open("offset.txt", 'w')

for i in range(len(offset_list)):
    fo.write(offset_list[i]+','+length_list[i]+'\n')

fo.close()