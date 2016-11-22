import xml.etree.ElementTree as ET
import numpy, os, sys, time, tarfile, configparser, datetime, math
import gzip

##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Version : 1.2.1
    Author : linxuteng
    E_mail : lxuteng@live.cn
--------------------------------
""")
print('\n')
exetime = int(time.strftime('%Y%m%d', time.localtime(time.time())))
if exetime > 20180101:
    print('\n')
    print('-' * 64)
    print('试用版本已过期，请联系作者！')
    print('-' * 64)
    print('\n')
    input()
    sys.exit()
print('\n')

print(
    '''
update log:

2016-6-1 12:17:11 bug修复；
2016-6-1 23:20:00 bug修复；
2016-6-14 17:37:51 支持MRO解码（GZ）；
2016-7-5 14:47:29 修改bug，遇到非法xml文件可以跳过；
2016-11-21 23:30:06 支持双层压缩嵌套tar.gz文件
'''
)

print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n\n')
time.sleep(1)

##################################################################################

start = time.time()

# 获取配置文件
inipath = os.path.split(os.path.abspath(sys.argv[0]))[0]
cf = configparser.ConfigParser()
cf.read(''.join((inipath, '\\', 'config.ini')), encoding='utf-8-SIG')

targz = cf.get('config', 'targz')
parse_type = cf.get('config', 'parse_type')
path = cf.get('config', 'path')
local_path = cf.get('config', 'local_path')
timing = cf.get('config', 'timing')

# MRS
parse_sumtype = cf.get('MRS', 'parse_sumtype')
parse_sheet = cf.get('MRS', 'parse_sheet').split(',')
exception_sheet = cf.get('MRS', 'exception_sheet').split(',')

# MRO
parse_sumtype_mro = cf.get('MRO', 'parse_sumtype')
# 门限2
if cf.get('MRO', 'overlap_db_1') == '':
    overlap_db_1 = ''
else:
    overlap_db_1 = int(cf.get('MRO', 'overlap_db_1'))

if cf.get('MRO', 'overlap_ncell_rsrp_1') == '':
    overlap_ncell_rsrp_1 = ''
else:
    overlap_ncell_rsrp_1 = int(cf.get('MRO', 'overlap_ncell_rsrp_1'))
# 门限2
if cf.get('MRO', 'overlap_db_2') == '':
    overlap_db_2 = ''
else:
    overlap_db_2 = int(cf.get('MRO', 'overlap_db_2'))

if cf.get('MRO', 'overlap_ncell_rsrp_2') == '':
    overlap_ncell_rsrp_2 = ''
else:
    overlap_ncell_rsrp_2 = int(cf.get('MRO', 'overlap_ncell_rsrp_2'))


# 取路径下面文件及文件夹
def local_dir(path):
    local_dir_list = []
    local_file_list = []
    for i in os.listdir(path):
        if os.path.isdir('\\'.join((path, i))):
            local_dir_list.append(i)
        else:
            local_file_list.append(i)
    return (local_dir_list, local_file_list)


local_path_list = local_dir(local_path)[0]

# 定时功能
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
    '%Y%m%d')
if timing == '1':
    path = '\\'.join((path, yesterday))
    local_path = '\\'.join((local_path, yesterday))
    if yesterday not in local_path_list:
        os.mkdir(local_path)


# 进度条
def progress(len_, count_):
    bar_len = 24
    hashes = '|' * int(count_ / len_ * bar_len)
    spaces = '_' * (bar_len - len(hashes))
    sys.stdout.write("\r%s %s %d%%" % (
        str(count_), hashes + spaces, int(count_ / len_ * 100)))
    sys.stdout.flush()


# 写入文件
def write_hour_all():
    for i in mr_date:
        f = open(''.join((local_path, '\\', i, '_hour.csv')), 'a')
        f.write('time,ECID,ENB_ID,ENB_CELLID,')
        if i == 'MR.RSRP':
            f.write('MR覆盖率(>=-110),>=-110,all,')
        for j in mr_head[i]:
            f.write(j)
            f.write(',')
        f.write('\n')

        for j in mr_date[i]:
            for k in mr_date[i][j]:
                f.write(j + ',' + k + ',')
                try:
                    f.write(str(int(k) // 256))
                    f.write(',')
                    f.write(
                        ''.join((str(int(k) // 256), '_', str(int(k) % 256))))
                    f.write(',')
                except:
                    f.write(str(int(k[:k.find(':')]) // 256))
                    f.write(',')
                    f.write(''.join((str(int(k[:k.find(':')]) // 256), '_',
                                     str(int(k[:k.find(':')]) % 256))))
                    f.write(',')
                if i == 'MR.RSRP':
                    if numpy.sum(mr_date[i][j][k]) == 0:
                        f.write('-')
                    else:
                        f.write(str(round(
                            numpy.sum(mr_date[i][j][k][7:]) / numpy.sum(
                                mr_date[i][j][k]) * 100, 2)))
                    f.write(''.join(
                        (
                            ',', str(numpy.sum(mr_date[i][j][k][7:])), ',',
                            str(numpy.sum(mr_date[i][j][k])),
                            ',')))
                    # f.write(',')
                    # f.write(str(numpy.sum(mr_date[i][j][k][7:])))
                    # f.write(',')
                    # f.write(str(numpy.sum(mr_date[i][j][k])))
                    # f.write(',')
                f.write(','.join(list(map(str, mr_date[i][j][k]))))
                f.write('\n')


def write_id():
    for i in mr_date:
        f = open(''.join((local_path, '\\', i, '.csv')), 'a')
        if timing == '1':
            f.write('日期,')
        f.write('ECID')
        f.write(',')
        f.write('ENB_ID')
        f.write(',')
        f.write('ENB_CELLID')
        f.write(',')
        if i == 'MR.RSRP':
            f.write('MR覆盖率(>=-110)')
            f.write(',')
            f.write('>=-110')
            f.write(',')
            f.write('all')
            f.write(',')
        for j in mr_head[i]:
            f.write(j)
            f.write(',')
        f.write('\n')

        for j in mr_date[i]:
            if timing == '1':
                f.write(yesterday)
                f.write(',')
            f.write(j)
            f.write(',')
            try:
                f.write(str(int(j) // 256))
                f.write(',')
                f.write(''.join((str(int(j) // 256), '_', str(int(j) % 256))))
                f.write(',')
            except:
                f.write(str(int(j[:j.find(':')]) // 256))
                f.write(',')
                f.write(''.join((str(int(j[:j.find(':')]) // 256), '_',
                                 str(int(j[:j.find(':')]) % 256))))
                f.write(',')
            if i == 'MR.RSRP':
                if numpy.sum(mr_date[i][j]) == 0:
                    f.write('-')
                else:
                    f.write(str(round(numpy.sum(mr_date[i][j][7:]) / numpy.sum(
                        mr_date[i][j]) * 100, 2)))
                f.write(',')
                f.write(str(numpy.sum(mr_date[i][j][7:])))
                f.write(',')
                f.write(str(numpy.sum(mr_date[i][j])))
                f.write(',')
            f.write(','.join(list(map(str, mr_date[i][j]))))
            f.write('\n')


def write_all():
    for i in mr_date:
        f = open(''.join((local_path, '\\', i, '.csv')), 'a')
        if timing == '1':
            f.write('日期,')
        f.write('ECID')
        f.write(',')
        f.write('ENB_ID')
        f.write(',')
        f.write('ENB_CELLID')
        f.write(',')
        if i == 'MR.RSRP':
            f.write('MR覆盖率(>=-110)')
            f.write(',')
            f.write('>=-110')
            f.write(',')
            f.write('all')
            f.write(',')
        for j in mr_head[i]:
            f.write(j)
            f.write(',')
        f.write('\n')

        for j in mr_date_sumid[i]:
            if timing == '1':
                f.write(yesterday)
                f.write(',')
            f.write(j)
            f.write(',')
            try:
                f.write(str(int(j) // 256))
                f.write(',')
                f.write(''.join((str(int(j) // 256), '_', str(int(j) % 256))))
                f.write(',')
            except:
                f.write(str(int(j[:j.find(':')]) // 256))
                f.write(',')
                f.write(''.join((str(int(j[:j.find(':')]) // 256), '_',
                                 str(int(j[:j.find(':')]) % 256))))
                f.write(',')
            if i == 'MR.RSRP':
                if numpy.sum(mr_date_sumid[i][j]) == 0:
                    f.write('-')
                else:
                    f.write(str(round(
                        numpy.sum(mr_date_sumid[i][j][7:]) / numpy.sum(
                            mr_date_sumid[i][j]) * 100, 2)))
                f.write(',')
                f.write(str(numpy.sum(mr_date_sumid[i][j][7:])))
                f.write(',')
                f.write(str(numpy.sum(mr_date_sumid[i][j])))
                f.write(',')
            f.write(','.join(list(map(str, mr_date_sumid[i][j]))))
            f.write('\n')


############################################################
# MRO相关
def Overlap(overlap_num, overlap_db, overlap_ncell_rsrp):
    if (l_list[11] != 0) and (l_list[9] >= (140 + overlap_ncell_rsrp)) and (
                (l_list[9] - l_list[0]) >= overlap_db) and (
                l_list[7] == l_list[11]):
        # 采样点
        if overlap_num == 0:
            try:
                mro_parse_res[k.attrib['id']][
                    '_'.join(('overlap', str(overlap_db),
                              str(overlap_ncell_rsrp)))] += 1
            except:
                mro_parse_res[k.attrib['id']]['_'.join(
                    ('overlap', str(overlap_db), str(overlap_ncell_rsrp)))] = 1
            overlap_num = 1
        try:
            mro_parse_res[k.attrib['id']][
                '_'.join(('overlap', str(overlap_db), str(overlap_ncell_rsrp),
                          's_cell_rsrp'))] = int((mro_parse_res[
                                                      k.attrib[
                                                          'id']][
                                                      '_'.join((
                                                          'overlap',
                                                          str(
                                                              overlap_db),
                                                          str(
                                                              overlap_ncell_rsrp),
                                                          's_cell_rsrp'))] +
                                                  l_list[0]) / 2)
            mro_parse_res[k.attrib['id']]['_'.join(('overlap', str(overlap_db),
                                                    str(overlap_ncell_rsrp),
                                                    'n_cell_rsrp'))] = int(
                (mro_parse_res[
                     k.attrib['id']][
                     '_'.join((
                         'overlap',
                         str(
                             overlap_db),
                         str(
                             overlap_ncell_rsrp),
                         'n_cell_rsrp'))] +
                 l_list[9]) / 2)
            mro_parse_res[k.attrib['id']]['_'.join(('overlap', str(overlap_db),
                                                    str(overlap_ncell_rsrp),
                                                    'ScTadv'))] = int(
                (mro_parse_res[
                     k.attrib['id']][
                     '_'.join((
                         'overlap',
                         str(
                             overlap_db),
                         str(
                             overlap_ncell_rsrp),
                         'ScTadv'))] +
                 l_list[2]) / 2)
        except:
            mro_parse_res[k.attrib['id']]['_'.join(
                ('overlap', str(overlap_db), str(overlap_ncell_rsrp),
                 's_cell_rsrp'))] = l_list[0]
            mro_parse_res[k.attrib['id']]['_'.join(
                ('overlap', str(overlap_db), str(overlap_ncell_rsrp),
                 'n_cell_rsrp'))] = l_list[9]
            mro_parse_res[k.attrib['id']]['_'.join(
                ('overlap', str(overlap_db), str(overlap_ncell_rsrp),
                 'ScTadv'))] = l_list[2]
    return overlap_num


def Ecid_ecid():
    def fun_minus11():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '<-10db'] += 1

    def fun_minus10():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-10db'] += 1

    def fun_minus9():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-9db'] += 1

    def fun_minus8():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-8db'] += 1

    def fun_minus7():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-7db'] += 1

    def fun_minus6():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-6db'] += 1

    def fun_minus5():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-5db'] += 1

    def fun_minus4():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-4db'] += 1

    def fun_minus3():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-3db'] += 1

    def fun_minus2():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-2db'] += 1

    def fun_minus1():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '-1db'] += 1

    def fun_0():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '0db'] += 1

    def fun_1():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '1db'] += 1

    def fun_2():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '2db'] += 1

    def fun_3():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '3db'] += 1

    def fun_4():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '4db'] += 1

    def fun_5():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '5db'] += 1

    def fun_6():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '6db'] += 1

    def fun_7():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '7db'] += 1

    def fun_8():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '8db'] += 1

    def fun_9():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '9db'] += 1

    def fun_10():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '10db'] += 1

    def fun_11():
        mro_parse_res[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci][
            '>10db'] += 1

    fun_list = {-11: fun_minus11, -10: fun_minus10, -9: fun_minus9,
                -8: fun_minus8, -7: fun_minus7, -6: fun_minus6,
                -5: fun_minus5, -4: fun_minus4, -3: fun_minus3, -2: fun_minus2,
                -1: fun_minus1, 0: fun_0, 1: fun_1,
                2: fun_2, 3: fun_3, 4: fun_4, 5: fun_5, 6: fun_6, 7: fun_7,
                8: fun_8, 9: fun_9, 10: fun_10, 11: fun_11}

    a = l_list[9] - l_list[0]
    try:
        fun_list[a]()
    except:
        if a < -10:
            a = -11
        elif a > 10:
            a = 11
        fun_list[a]()


# 建立earfcn pci cellid 索引表
def earfcn_pci_cellid_relate():
    global earfcn_pci_cellid
    global enbid_list
    f = open(r"D:\python\1_tools\MR_parse\enb_basedat.csv", 'r')
    all = [i.rstrip().split(',') for i in f.readlines()]
    earfcn_pci_cellid = {}
    enbid_list = {}
    for k in all:
        if k[7] not in earfcn_pci_cellid:
            earfcn_pci_cellid[k[7]] = {}
        if k[6] not in earfcn_pci_cellid[k[7]]:
            earfcn_pci_cellid[k[7]][k[6]] = []
        try:
            earfcn_pci_cellid[k[7]][k[6]].append(
                (k[0], k[1], float(k[4]), float(k[5])))
        except:
            pass

        if k[0] not in enbid_list:
            try:
                enbid_list[k[0]] = (float(k[4]), float(k[5]))
            except:
                pass
    all = ''


# 计算距离
def distance(lon_1, lat_1, lon_2, lat_2):
    lon1, lat1, lon2, lat2 = map(math.radians, [lon_1, lat_1, lon_2, lat_2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(
        dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000


def min_distance_cell(s_enbid, n_earfcn, n_pci):
    min_distance_list = {}
    try:
        for i in earfcn_pci_cellid[n_earfcn][n_pci]:
            min_distance_list[
                distance(enbid_list[s_enbid][0], enbid_list[s_enbid][1], i[2],
                         i[3])] = i[1]
            # print(str(min_distance_list))
            min_stance = min(min_distance_list)
            min_cell = min_distance_list[min_stance]
            min_stance = int(min_stance)
    except:
        min_cell = '-'
        min_stance = '-'
    return min_cell, min_stance


if targz.lower() in ('xml', ''):
    if parse_type.lower() in ('MRS', 'all', 'mrs'):
        # 获取路径下所有文件
        file_list = []
        print('>>> 开始获取文件，请稍后...')
        for root, dirs, files in os.walk(path):
            for file in files:
                if ('MRS' in file or 'mrs' in file) and '.xml' in file.lower():
                    file_list.append(os.path.join(root, file))
        if len(file_list) != 0:
            print('>>> 获取到文件：', len(file_list))
        else:
            print('>>> 未获取到文件，请检查！！')
            sys.exit()
        file_num = len(file_list)
        # 获取表头及处理列表
        mr_head = {}
        tree = ET.parse(file_list[0])
        # root = tree.getroot()
        # 筛选目标处理列表
        if '' in parse_sheet:
            mrtype = [i.attrib['mrName'] for i in tree.iter(tag='measurement')]
        else:
            mrtype = parse_sheet
        mrtype = [i for i in mrtype if i not in exception_sheet]

        for i in tree.iter('measurement'):
            if i.attrib['mrName'] in mrtype:
                if i.attrib['mrName'] not in mr_head:
                    for j in i:
                        if j.tag == 'smr':
                            mr_head[i.attrib['mrName']] = j.text.split(' ')

        # 解码
        mr_date = {}
        print('>>> 解码开始，请稍后...')
        timerun = 0
        if parse_sumtype in ('hour', 'all'):
            for file_ob in file_list:
                tree = ET.parse(file_ob)
                # root = tree.getroot()
                for mr_reporttime_ob in tree.iter(tag='fileHeader'):
                    mr_reporttime = mr_reporttime_ob.attrib['reportTime'][
                                    :mr_reporttime_ob.attrib['reportTime'].find(
                                        ':')]
                    break
                for i in tree.iter('measurement'):
                    if i.attrib['mrName'] in mrtype:
                        if i.attrib['mrName'] not in mr_date:
                            mr_date[i.attrib['mrName']] = {}
                        if mr_reporttime not in mr_date[i.attrib['mrName']]:
                            mr_date[i.attrib['mrName']][mr_reporttime] = {}
                        for j in i:
                            if j.tag == 'smr':
                                pass
                            else:
                                for k in j:
                                    try:
                                        mr_date[i.attrib['mrName']][
                                            mr_reporttime][
                                            j.attrib['id']] += numpy.array(
                                            list(map(int, k.text.rstrip().split(
                                                ' '))))
                                    except:
                                        mr_date[i.attrib['mrName']][
                                            mr_reporttime][
                                            j.attrib['id']] = numpy.array(
                                            list(map(int, k.text.rstrip().split(
                                                ' '))))
                # 更新进度条
                timerun += 1
                progress(file_num, timerun)
        elif parse_sumtype == 'id':
            for file_ob in file_list:
                tree = ET.parse(file_ob)
                # root = tree.getroot()
                for i in tree.iter('measurement'):
                    if i.attrib['mrName'] in mrtype:
                        if i.attrib['mrName'] not in mr_date:
                            mr_date[i.attrib['mrName']] = {}
                        for j in i:
                            if j.tag == 'smr':
                                pass
                            else:
                                for k in j:
                                    try:
                                        mr_date[i.attrib['mrName']][
                                            j.attrib['id']] += numpy.array(
                                            list(map(int, k.text.rstrip().split(
                                                ' '))))
                                    except:
                                        mr_date[i.attrib['mrName']][
                                            j.attrib['id']] = numpy.array(
                                            list(map(int, k.text.rstrip().split(
                                                ' '))))
                # 更新进度条
                timerun += 1
                progress(file_num, timerun)
        else:
            pass
        print('\n>>> 解码完成！')
        end = time.time()
        print('>>> 解码历时：', int(end - start), 's\n')
        print('-' * 4)
        print('>>> 开始生成结果文档，请稍后....')

        # 写入文件
        if parse_sumtype in ('hour', 'all'):
            write_hour_all()
        elif parse_sumtype == 'id':
            write_id()

        # 汇总hour 到 id
        if parse_sumtype == 'all':
            mr_date_sumid = {}
            for i in mr_date:
                if i not in mr_date_sumid:
                    mr_date_sumid[i] = {}
                for j in mr_date[i]:
                    for k in mr_date[i][j]:
                        try:
                            mr_date_sumid[i][k] += mr_date[i][j][k]
                        except:
                            mr_date_sumid[i][k] = mr_date[i][j][k]
                        mr_date[i][j][k] = ''
            # 写入文件
            write_all()

        end1 = time.time()
        print('>>> 完成！')
        print('>>> 生成文档历时：', int(end1 - end), 's')
        print('-' * 32)
        print('>>> 总历时：', int(end1 - start), 's')
        print('-' * 32)
    elif parse_type in ('MRO', 'all'):
        print('暂不支持解码MRO！')

elif targz.lower() == 'gz':
    if parse_type.lower() in ('MRS', 'all', 'mrs'):
        tar_list = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if ('mrs' in file or 'MRS' in file) and '.tar.gz' in file:
                    tar_list.append(os.path.join(root, file))
        if len(tar_list) != 0:
            print('>>> 获取到 tar.gz 文件：', len(tar_list))
        else:
            print('>>> 未获取到文件，请检查！！')
            sys.exit()
        print('>>> 分析 tar.gz 文件开始...')
        tar_iter_num = 0
        progress_num = 0
        for tar_iter in tar_list:
            progress(len(tar_list), progress_num)
            tar_f = tarfile.open(tar_iter)
            tar_iter_num += len(tar_f.getnames())
            progress_num += 1
        progress(len(tar_list), progress_num)
        print('\n')
        print('>>> 共获取到子文件：', tar_iter_num)
        if tar_iter_num == 0:
            print('>>> 未获取到子文件，程序结束！！')
            sys.exit()
        print('>>> 解码开始，请稍后...')
        # 获取表头及处理列表
        mr_head = {}
        try:
            tar_f = tarfile.open(tar_list[0])
        except:
            pass
        for file_ob in tar_f.getnames():
            try:
                tar_temp = tar_f.extractfile(file_ob)
            except:
                continue
            if file_ob.split('.')[-1] == 'gz':
                try:
                    tree = ET.parse(gzip.open(tar_temp))
                except:
                    continue
            elif file_ob.split('.')[-1] == 'xml':
                try:
                    tree = ET.parse(tar_temp)
                except:
                    continue
            # root = tree.getroot()
            # 筛选目标处理列表
            if '' in parse_sheet:
                mrtype = [i.attrib['mrName'] for i in
                          tree.iter(tag='measurement')]
            else:
                mrtype = parse_sheet
            mrtype = [i for i in mrtype if i not in exception_sheet]

            for i in tree.iter('measurement'):
                if i.attrib['mrName'] in mrtype:
                    if i.attrib['mrName'] not in mr_head:
                        for j in i:
                            if j.tag == 'smr':
                                mr_head[i.attrib['mrName']] = j.text.split(' ')
            break

        mr_date = {}
        timerun = 0
        if parse_sumtype in ('hour', 'all'):
            for tar_iter in tar_list:
                try:
                    tar_f = tarfile.open(tar_iter)
                except:
                    continue
                for file_ob in tar_f.getnames():
                    try:
                        tar_temp = tar_f.extractfile(file_ob)
                    except:
                        continue
                    if file_ob.split('.')[-1] == 'gz':
                        try:
                            tree = ET.parse(gzip.open(tar_temp))
                        except:
                            continue
                    elif file_ob.split('.')[-1] == 'xml':
                        try:
                            tree = ET.parse(tar_temp)
                        except:
                            continue
                        # root = tree.getroot()
                    for mr_reporttime_ob in tree.iter(tag='fileHeader'):
                        mr_reporttime = mr_reporttime_ob.attrib[
                                            'reportTime'][
                                        :mr_reporttime_ob.attrib[
                                            'reportTime'].find(':')]
                        break
                    for i in tree.iter('measurement'):
                        if i.attrib['mrName'] in mrtype:
                            if i.attrib['mrName'] not in mr_date:
                                mr_date[i.attrib['mrName']] = {}
                            if mr_reporttime not in mr_date[
                                i.attrib['mrName']]:
                                mr_date[i.attrib['mrName']][
                                    mr_reporttime] = {}
                            for j in i:
                                if j.tag == 'smr':
                                    pass
                                else:
                                    for k in j:
                                        try:
                                            mr_date[i.attrib['mrName']][
                                                mr_reporttime][j.attrib[
                                                'id']] += numpy.array(
                                                list(map(int,
                                                         k.text.rstrip().split(
                                                             ' '))))
                                        except:
                                            mr_date[i.attrib['mrName']][
                                                mr_reporttime][j.attrib[
                                                'id']] = numpy.array(
                                                list(map(int,
                                                         k.text.rstrip().split(
                                                             ' '))))
                    # 更新进度条
                    timerun += 1
                    progress(tar_iter_num, timerun)
        elif parse_sumtype == 'id':
            for tar_iter in tar_list:
                try:
                    tar_f = tarfile.open(tar_iter)
                except:
                    continue
                for file_ob in tar_f.getnames():
                    try:
                        tar_temp = tar_f.extractfile(file_ob)
                    except:
                        continue
                    if file_ob.split('.')[-1] == 'gz':
                        try:
                            tree = ET.parse(gzip.open(tar_temp))
                        except:
                            continue
                    elif file_ob.split('.')[-1] == 'xml':
                        try:
                            tree = ET.parse(tar_temp)
                        except:
                            continue
                    for i in tree.iter('measurement'):
                        if i.attrib['mrName'] in mrtype:
                            if i.attrib['mrName'] not in mr_date:
                                mr_date[i.attrib['mrName']] = {}
                            for j in i:
                                if j.tag == 'smr':
                                    pass
                                else:
                                    for k in j:
                                        try:
                                            mr_date[i.attrib['mrName']][
                                                j.attrib[
                                                    'id']] += numpy.array(
                                                list(map(int,
                                                         k.text.rstrip().split(
                                                             ' '))))
                                        except:
                                            mr_date[i.attrib['mrName']][
                                                j.attrib[
                                                    'id']] = numpy.array(
                                                list(map(int,
                                                         k.text.rstrip().split(
                                                             ' '))))

                    # 更新进度条
                    timerun += 1
                    progress(tar_iter_num, timerun)
        else:
            pass
        print('\n>>> 解码完成！')
        end = time.time()
        print('>>> 解码历时：', int(end - start), 's\n')
        print('-' * 4)
        print('>>> 开始生成结果文档，请稍后....')

        # 写入文件
        if parse_sumtype in ('hour', 'all'):
            write_hour_all()
        elif parse_sumtype == 'id':
            write_id()

        # 汇总hour 到 id
        if parse_sumtype == 'all':
            mr_date_sumid = {}
            for i in mr_date:
                if i not in mr_date_sumid:
                    mr_date_sumid[i] = {}
                for j in mr_date[i]:
                    for k in mr_date[i][j]:
                        try:
                            mr_date_sumid[i][k] += mr_date[i][j][k]
                        except:
                            mr_date_sumid[i][k] = mr_date[i][j][k]
                        mr_date[i][j][k] = ''
            # 写入文件
            write_all()

        end1 = time.time()

        print('>>> 完成！')
        print('>>> 生成文档历时：', int(end1 - end), 's')
        print('-' * 32)
        print('>>> 总历时：', int(end1 - start), 's')
        print('-' * 32)

    elif parse_type in ('MRO', 'all', 'mro', 'ALL'):
        tar_list = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if 'mro' in file.lower() and '.tar.gz' in file.lower():
                    tar_list.append(os.path.join(root, file))
        if len(tar_list) != 0:
            print('>>> 获取到 tar.gz 文件：', len(tar_list))
        else:
            print('>>> 未获取到文件，请检查！！')
            sys.exit()
        print('>>> 预解码开始，请稍等...')
        tar_iter_num = 0
        progress_num = 0
        for tar_iter in tar_list:
            progress(len(tar_list), progress_num)
            tar_f = tarfile.open(tar_iter)
            tar_iter_num += len(tar_f.getnames())
            progress_num += 1
        progress(len(tar_list), progress_num)
        print('\n')
        print('>>> 共获取到子文件：', tar_iter_num)
        parse_time_tar = time.time()
        print('>>> 历时：', int(parse_time_tar - start))
        if tar_iter_num == 0:
            print('>>> 未获取到子文件，程序结束！！')
            sys.exit()
        print('>>> 解码开始，请稍后...')
        print('\n')
        # 获取表头及处理列表
        mr_head = ''
        tar_f = tarfile.open(tar_list[0])
        for i in tar_f:
            tree = ET.parse(tar_f.extractfile(i))
            # root = tree.getroot()
            for i in tree.iter('measurement'):
                for j in i:
                    if j.text[:12] == 'MR.LteScRSRP':
                        mr_head = j.text.split(' ')
                        break
                break
            break
        mro_parse_res = {}
        # 进度条计数器
        mro_progress_num = 0
        for i in tar_list:
            tar_f = tarfile.open(i)
            for j in tar_f:
                tree = ET.parse(tar_f.extractfile(j))
                root = tree.getroot()
                # MRO共有三个measurement，但是均未命名，因此需先判断哪个measurement是所需要提取的数据
                measurement_len = len([i for i in tree.iter('measurement')])
                for measurement_len_ob in range(measurement_len):
                    if root[1][measurement_len_ob][0].tag == 'smr' and \
                                    root[1][measurement_len_ob][0].text[
                                    :12] == 'MR.LteScRSRP':
                        for k in root[1][measurement_len_ob].iter('object'):
                            if k.attrib['id'] not in mro_parse_res:
                                mro_parse_res[k.attrib['id']] = {
                                    's_samplint': 0, 'ECID_ECID': {}}
                            l_num = 0
                            overlap_num_1 = 0
                            overlap_num_2 = 0
                            for l in k:
                                l_list = numpy.array(list(map(int,
                                                              l.text.replace(
                                                                  'NIL',
                                                                  '0').rstrip().split(
                                                                  ' '))))
                                # 计算主小区采样点及采样信息
                                if l_num == 0:
                                    mro_parse_res[k.attrib['id']][
                                        's_samplint'] += 1
                                    try:
                                        mro_parse_res[k.attrib['id']][
                                            's_basic'] += numpy.concatenate(
                                            (l_list[0:9], l_list[20:22],
                                             l_list[23:27]))
                                        mro_parse_res[k.attrib['id']][
                                            's_basic'] = \
                                            mro_parse_res[k.attrib['id']][
                                                's_basic'] // numpy.array(
                                                [2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                                                 2, 2,
                                                 2, 2, 2])
                                    except:
                                        mro_parse_res[k.attrib['id']][
                                            's_basic'] = numpy.concatenate(
                                            (l_list[0:9], l_list[20:22],
                                             l_list[23:27]))
                                    l_num = 1
                                # 计算重叠覆盖率_1
                                if overlap_db_1 != '' and overlap_ncell_rsrp_1 != '':
                                    overlap_num_1 = Overlap(overlap_num_1,
                                                            overlap_db_1,
                                                            overlap_ncell_rsrp_1)
                                # 计算重叠覆盖率_2
                                if overlap_db_2 != '' and overlap_ncell_rsrp_2 != '':
                                    overlap_num_2 = Overlap(overlap_num_2,
                                                            overlap_db_2,
                                                            overlap_ncell_rsrp_2)
                                # 计算与邻区关系
                                n_cell_earfcn_pci = '_'.join(
                                    list(map(str, l_list[11:13])))
                                if l_list[11] != 0:
                                    if n_cell_earfcn_pci not in \
                                            mro_parse_res[k.attrib['id']][
                                                'ECID_ECID']:
                                        mro_parse_res[k.attrib['id']][
                                            'ECID_ECID'][n_cell_earfcn_pci] = {
                                            'ncrsrp': l_list[9],
                                            'scrsrp': l_list[0],
                                            'ScTadv': l_list[2],
                                            'n_samplint': 0, '<-10db': 0,
                                            '-10db': 0, '-9db': 0, '-8db': 0,
                                            '-7db': 0,
                                            '-6db': 0, '-5db': 0, '-4db': 0,
                                            '-3db': 0, '-2db': 0, '-1db': 0,
                                            '0db': 0,
                                            '1db': 0, '2db': 0, '3db': 0,
                                            '4db': 0, '5db': 0, '6db': 0,
                                            '7db': 0,
                                            '8db': 0, '9db': 0, '10db': 0,
                                            '>10db': 0}
                                        Ecid_ecid()
                                    else:
                                        mro_parse_res[k.attrib['id']][
                                            'ECID_ECID'][n_cell_earfcn_pci][
                                            'ncrsrp'] = (
                                                            mro_parse_res[
                                                                k.attrib[
                                                                    'id']][
                                                                'ECID_ECID'][
                                                                n_cell_earfcn_pci][
                                                                'ncrsrp'] +
                                                            l_list[
                                                                9]) // 2
                                        mro_parse_res[k.attrib['id']][
                                            'ECID_ECID'][n_cell_earfcn_pci][
                                            'scrsrp'] = (
                                                            mro_parse_res[
                                                                k.attrib[
                                                                    'id']][
                                                                'ECID_ECID'][
                                                                n_cell_earfcn_pci][
                                                                'scrsrp'] +
                                                            l_list[
                                                                0]) // 2
                                        mro_parse_res[k.attrib['id']][
                                            'ECID_ECID'][n_cell_earfcn_pci][
                                            'ScTadv'] = (
                                                            mro_parse_res[
                                                                k.attrib[
                                                                    'id']][
                                                                'ECID_ECID'][
                                                                n_cell_earfcn_pci][
                                                                'ScTadv'] +
                                                            l_list[
                                                                2]) // 2
                                        Ecid_ecid()
                                    mro_parse_res[k.attrib['id']]['ECID_ECID'][
                                        n_cell_earfcn_pci]['n_samplint'] += 1

                        # 获取到所需要的measurement后，则退出循环
                        break
                ##更新进度条
                mro_progress_num += 1
                progress(tar_iter_num, mro_progress_num)
        print('\n')
        parst_time = time.time()
        print('>>> 解码历时：', int(parst_time - parse_time_tar))
        # 生成结果文件main
        print('>>> 开始生成解码结果，请稍后...')
        # 重命名counter名称（太长了不好看）
        overlap_1_s = '_'.join(('overlap', str(overlap_db_1),
                                str(overlap_ncell_rsrp_1), 's_cell_rsrp'))
        overlap_1_n = '_'.join(('overlap', str(overlap_db_1),
                                str(overlap_ncell_rsrp_1), 'n_cell_rsrp'))
        overlap_1_ta = '_'.join(
            ('overlap', str(overlap_db_1), str(overlap_ncell_rsrp_1), 'ScTadv'))
        overlap_1_samplint = '_'.join(('overlap', str(overlap_db_1),
                                       str(overlap_ncell_rsrp_1), 'samplint'))
        overlap_2_samplint = '_'.join(('overlap', str(overlap_db_2),
                                       str(overlap_ncell_rsrp_2), 'samplint'))
        overlap_2_s = '_'.join(('overlap', str(overlap_db_2),
                                str(overlap_ncell_rsrp_2), 's_cell_rsrp'))
        overlap_2_n = '_'.join(('overlap', str(overlap_db_2),
                                str(overlap_ncell_rsrp_2), 'n_cell_rsrp'))
        overlap_2_ta = '_'.join(
            ('overlap', str(overlap_db_2), str(overlap_ncell_rsrp_2), 'ScTadv'))
        # 与解码结果保持一致
        overlap_1_sam = '_'.join(
            ('overlap', str(overlap_db_1), str(overlap_ncell_rsrp_1)))
        overlap_2_sam = '_'.join(
            ('overlap', str(overlap_db_2), str(overlap_ncell_rsrp_2)))
        # 生成文件
        mro_main_f = open(''.join((local_path, '\\', 'MRO_main.csv')), 'a')
        # 写入表头
        mro_main_f.write(
            'ECID,ENB_CELL,MR.LteScRSRP,MR.LteScRSRQ,MR.LteScTadv,MR.LteSceNBRxTxTimeDiff,MR.LteScPHR,MR.LteScAOA,MR.LteScSinrUL,MR.LteScEarfcn,MR.LteScPci,MR.LteScPUSCHPRBNum,MR.LteScPDSCHPRBNum,MR.LteScRI1,MR.LteScRI2,MR.LteScRI4,MR.LteScRI8,s_samplint,')
        mro_main_f.write(','.join((overlap_1_samplint, overlap_1_s, overlap_1_n,
                                   overlap_1_ta, overlap_2_samplint,
                                   overlap_2_s, overlap_2_n, overlap_2_ta,
                                   '\n')))
        # 写入解码结果
        for a in mro_parse_res:
            mro_main_f.write(a)
            mro_main_f.write(',')
            mro_main_f.write('_'.join((str(int(a) // 256), str(int(a) % 256))))
            mro_main_f.write(',')
            mro_main_f.write(
                ','.join(list(map(str, mro_parse_res[a]['s_basic']))))
            mro_main_f.write(',')
            mro_main_f.write(str(mro_parse_res[a]['s_samplint']))
            mro_main_f.write(',')
            for b in (overlap_1_sam, overlap_1_s, overlap_1_n, overlap_1_ta,
                      overlap_2_sam, overlap_2_s, overlap_2_n,
                      overlap_2_ta):
                try:
                    if b in (
                            overlap_1_s, overlap_1_n, overlap_2_s, overlap_2_n):
                        mro_main_f.write(str(mro_parse_res[a][b] - 140))
                    else:
                        mro_main_f.write(str(mro_parse_res[a][b]))
                except:
                    mro_main_f.write('-')
                mro_main_f.write(',')
            mro_main_f.write('\n')

        # 读取小区基础数据文件
        earfcn_pci_cellid_relate()

        # 生成结果文件ECID
        # 生成文件
        mro_main_f = open(''.join((local_path, '\\', 'MRO_ECID.csv')), 'w')
        # 写入表头
        mro_main_f.write('s_ECID')
        mro_main_f.write(',ENB_CELL,s_earfcn,s_pci,n_ENB_CELL,')
        mro_main_f.write('n_earfcn,n_pci,s_n_distance(m)')
        mro_main_f.write(
            ',Scrsrp,ScTadv,n_samplint,ncrsrp,<-10db,-10db,-9db,-8db,-7db,-6db,-5db,-4db,-3db,-2db,-1db,0db,1db,2db,3db,4db,5db,6db,7db,8db,9db,10db,>10db\n')
        # 写入结果文件
        for c in mro_parse_res:
            for d in mro_parse_res[c]['ECID_ECID']:
                mro_main_f.write(c)
                mro_main_f.write(',')
                mro_main_f.write(
                    '_'.join((str(int(c) // 256), str(int(c) % 256))))
                mro_main_f.write(',')
                mro_main_f.write(str(mro_parse_res[c]['s_basic'][7]))
                mro_main_f.write(',')
                mro_main_f.write(str(mro_parse_res[c]['s_basic'][8]))
                mro_main_f.write(',')

                earfcn_pci = d.split('_')
                min_cell, min_stance = min_distance_cell(str(int(c) // 256),
                                                         earfcn_pci[0],
                                                         earfcn_pci[1])
                mro_main_f.write(min_cell)

                mro_main_f.write(',')
                mro_main_f.write(','.join(earfcn_pci))
                mro_main_f.write(',')
                mro_main_f.write(str(min_stance))
                mro_main_f.write(',')
                for e in (
                        'scrsrp', 'ScTadv', 'n_samplint', 'ncrsrp', '<-10db',
                        '-10db', '-9db', '-8db', '-7db', '-6db',
                        '-5db',
                        '-4db', '-3db', '-2db', '-1db', '0db', '1db', '2db',
                        '3db', '4db', '5db', '6db', '7db', '8db',
                        '9db',
                        '10db', '>10db'):
                    mro_main_f.write(str(mro_parse_res[c]['ECID_ECID'][d][e]))
                    mro_main_f.write(',')
                mro_main_f.write('\n')
        csv_time = time.time()
        print('>>> done!')
        print('>>> 生成结果历时：', int(csv_time - parst_time))
        print('-' * 32)
        print('>>> All done!')
        print('>>> 总历时：', int(csv_time - start))
        print('-' * 32)
else:
    print('targz 设置异常，请检查！！')
