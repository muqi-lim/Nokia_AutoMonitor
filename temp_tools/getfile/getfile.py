import os
import sys

path = os.path.split(os.path.abspath(sys.argv[0]))[0]


def walk(path):
    for i in os.listdir(path):
        full_i = '/'.join((path, i))
        if os.path.isdir(full_i):
            walk(full_i)
        else:
            if i != 'getfile.exe':
                filelist.append(full_i)


filelist = []
walk(path)

f = open(''.join((path, '/', 'filelist.csv')), 'w')
f.write('ip,vendor_filename\n')
for j in filelist:
    temp_filename = os.path.split(j)
    f.write(os.path.split(temp_filename[0])[1])
    f.write(',')
    f.write(temp_filename[1])
    f.write('\n')
f.close
