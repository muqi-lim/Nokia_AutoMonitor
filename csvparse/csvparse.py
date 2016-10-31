# -*- coding: utf-8 -*-
import copy, sys

f = open(
    r'D:\1_Work\4、潮州LTE工程项目\3、工作内容\4、日常\2016年9月28日 告警参数对应表\7562_1.csv',
    'r',
    encoding='utf-8-sig')

# ff = open(r"F:\2.csv", 'r', encoding='utf-8-sig')
ff = open(r"D:\1_Work\4、潮州LTE工程项目\3、工作内容\4、日常\2016年9月28日 告警参数对应表\kpi_HOUR_15A_20160901_LTECP2.csv",'r',encoding='utf-8-sig')

kpi = {}
for i in ff.readlines():
    line = i.split(',')
    # print(line)
    try:
        data = (float(line[13]) + float(line[14])) / 1024
        maxue = float(line[39])
        aveue = float(line[18])
        # print(data, maxue, aveue)
        if line[0] not in kpi:
            kpi[line[0]] = {}
        if line[3] not in kpi[line[0]]:
            kpi[line[0]][line[3]] = copy.deepcopy([data, maxue, aveue])
        else:
            data = data + kpi[line[0]][line[3]][0]
            maxue = max(maxue, kpi[line[0]][line[3]][1])
            aveue = max(aveue, kpi[line[0]][line[3]][2])
            kpi[line[0]][line[3]] = copy.deepcopy([data, maxue, aveue])
    except:
        pass
ff.close
fff = open(
    r'D:\1_Work\4、潮州LTE工程项目\3、工作内容\4、日常\2016年9月28日 告警参数对应表\1.csv',
    'w',
    encoding='utf-8')
for i in f.readlines():
    f_data = i.rstrip().split(',')
    fff.write(','.join(f_data))
    fff.write(',')
    try:
        f_data_head = f_data[0].split('_')
        # print(f_data_head)
        kpi_temp = ','.join(map(str, kpi[f_data_head[0]][f_data_head[1]]))
        fff.write(kpi_temp)
    except:
        pass
    fff.write('\n')
fff.close

for i in range(10):
    print(i)
def a():
    print('hello world')