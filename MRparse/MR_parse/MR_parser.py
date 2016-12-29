import xml.etree.ElementTree as ET
import numpy
import configparser
import os
import sys
import datetime
import gzip
import tarfile
import time
import math

# import copy

##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Author : lin_xu_teng
    E_mail : lxuteng@live.cn
--------------------------------
""")
print('\n')
auth_time = int(time.strftime('%Y%m%d', time.localtime(time.time())))
if auth_time > 20180101:
    print('\n')
    print('-' * 64)
    print('试用版本已过期，请联系作者！')
    print('-' * 64)
    print('\n')
    input()
    sys.exit()

print('''
update log:

2016-11-23 v1重构
2016-11-29 重构完成MRS解码
2016-11-30 修复定时任务bug
2016-12-2 支持MRO解码
2016-12-29 新增解码MRO_EARFCN表，统计采集到的各个邻区频点的RSRP段信息；

''')
print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n')
time.sleep(1)


##############################################################################


# noinspection PyAttributeOutsideInit
class ConfigManager:
    def __init__(self):
        """获取 main 配置参数"""
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((self.main_path, '\\', 'config.ini')),
                     encoding='utf-8-SIG')
        # 初始化配置列表
        self.config_main = {}
        self.config_mrs = {}
        self.config_mro = {}

        self.yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

        # 获取 main 配置
        self.get_main_config()

    def get_main_config(self):

        """获取 main 配置文件"""

        for a in self.cf.options('main'):
            self.config_main[a] = self.cf.get('main', a).split(',')

        # 部分配置特殊设置
        self.config_main['parse_type'] = [j.lower() for j in self.config_main['parse_type']]
        if self.config_main['parse_type'] == ['all']:
            self.config_main['parse_type'] = ['mrs', 'mro']

        self.config_main['file_type'] = [k.lower() for k in self.config_main['file_type']]
        if self.config_main['file_type'] == ['']:
            self.config_main['file_type'] = ['gz', 'xml']

        # 当启用定时功能时，目录重定向到 原设置目录/昨天 文件夹下
        if self.config_main['timing'][0] == '1':
            self.config_main['source_path'][0] = '\\'.join((self.config_main['source_path'][0], self.yesterday))
            self.config_main['target_path'][0] = '\\'.join((self.config_main['target_path'][0], self.yesterday))

        # 获取处理列表
        self.parse_file_list = {}
        self.get_files(self.config_main['source_path'][0])

    def get_mrs_config(self):
        """获取 MRS 配置文件"""
        for l in self.cf.options('MRS'):
            self.config_mrs[l] = self.cf.get('MRS', l).split(',')
        self.config_mrs['gather_type'] = [m.lower() for m in self.config_mrs['gather_type']]
        if self.config_mrs['gather_type'] == ['all']:
            self.config_mrs['gather_type'] = ['hour', 'id']
        if self.config_mrs['gather_type'] == ['id', 'hour']:
            self.config_mrs['gather_type'] = ['hour', 'id']

    def get_mro_config(self):
        """获取 MRO 配置文件"""
        for n in self.cf.options('MRO'):
            self.config_mro[n] = self.cf.get('MRO', n).split(',')

    @staticmethod
    def star_mrs_manager():
        """MRS解码"""
        print('>>> 解码 MRS 数据...')
        mrs_manager = MrsParser()
        config_manager.parse_process('mrs', mrs_manager)
        print('>>> MRS数据处理及保存，请等待...')
        num_i = len(config_manager.config_mrs['gather_type'])
        if num_i == 1:
            mrs_manager.data_writer(config_manager.config_mrs['gather_type'][0])
        elif num_i == 2:
            mrs_manager.data_writer('hour')
            mrs_manager.parser_plus()
            mrs_manager.data_writer('id')
        print('>>> MRS 数据处理完毕！')
        print('-' * 26)
        print('完成！MRS解码结果保存在此文件夹: ', config_manager.config_main['target_path'][0])
        print('-' * 26)

    @staticmethod
    def star_mro_manager():
        """MRO解码"""
        print('>>> 解码 MRO 数据...')
        mro_manager = MroParser()
        config_manager.parse_process('mro', mro_manager)
        print('>>> MRO数据处理及保存，请等待...')
        mro_manager.writer()
        print('>>> MRS 数据处理完毕！')
        print('-' * 26)
        print('完成！MRO解码结果保存在此文件夹: ', config_manager.config_main['target_path'][0])
        print('-' * 26)

    def parse_process(self, filetype, parsetype_class):

        """处理流程（文件格式判断、解压、parse xml）"""

        parse_file_list = self.parse_file_list[filetype]
        # 统计个数
        num_n = 0
        num_run = 0
        for temp_n in parse_file_list:
            num_n += len(parse_file_list[temp_n])
        # parse_file_list 格式为 {'mrs':{'gz':[...],'xml':[...]},
        #                         'mro':{'gz':[...],'xml':[...]},
        #                        }
        # temp_i 为文件类型（gz or xml）
        # temp_j 为待处理文件
        for n in parse_file_list:
            for o in parse_file_list[n]:
                # 进度条
                config_manager.progress(num_n, num_run, os.path.split(o)[-1])
                num_run += 1
                if n == 'xml':
                    try:
                        tree = ET.parse(o)
                        parsetype_class.parser(tree)
                    except:
                        continue
                elif n == 'gz':
                    try:
                        try:
                            tar_f = tarfile.open(o)
                            for temp_file in tar_f.getnames():
                                temp_file_tar_f = tar_f.extractfile(temp_file)
                                temp_file_suffix = temp_file.split('.')[-1].lower()
                                if temp_file_suffix == 'gz':
                                    tree = ET.parse(gzip.open(temp_file_tar_f))
                                    parsetype_class.parser(tree)
                                elif temp_file_suffix == 'xml':
                                    tree = ET.parse(temp_file_tar_f)
                                    parsetype_class.parser(tree)
                        except:
                            tree = ET.parse(gzip.open(o))
                            parsetype_class.parser(tree)
                    except:
                        continue
        # 完成进度条
        config_manager.progress(num_n, num_run, 'done!\n')

    def get_files(self, path):

        """获取需处理文件"""

        if not os.path.exists(path):
            print('source_path 所设置的目录不存在，请检查！')
            sys.exit()
        for root, dirs, files in os.walk(path):
            for temp_file in files:
                temp_parse_type_num = 0
                for temp_parse_type in self.config_main['parse_type']:
                    if temp_parse_type in temp_file.lower():
                        temp_parse_type_num = 1
                        temp_file_plus = [temp_file, temp_parse_type]
                        if temp_parse_type not in self.parse_file_list:
                            self.parse_file_list[temp_parse_type] = {}
                if temp_parse_type_num == 0:
                    continue
                # 判断文件类型（gz or xml）
                # 判断是否有文件后缀，如果没有，则跳过
                if '.' not in temp_file:
                    continue
                else:
                    file_type = temp_file.split('.')[-1].lower()
                    # 如果不是 file_type 设置的文件格式，则跳过
                    if file_type not in self.config_main['file_type']:
                        continue
                    # 如果有且满足file_type设置的文件格式，则加入到parse_file_list中
                    if file_type not in self.parse_file_list[temp_file_plus[1]]:
                        self.parse_file_list[temp_file_plus[1]][file_type] = []
                source_path = os.path.join(root, temp_file)
                self.parse_file_list[temp_file_plus[1]][file_type].append(source_path)
        if len(self.parse_file_list) == 0:
            print('未获取到源文件，请检查source_path是否设置正确或源文件是否存在！')
            sys.exit()

    def makedir(self):
        if not os.path.exists(self.config_main['target_path'][0]):
            os.makedirs(self.config_main['target_path'][0])

    # 进度条
    @staticmethod
    def progress(num_total, num_run, file_name=''):
        bar_len = 10
        hashes = '|' * int(num_run / num_total * bar_len)
        spaces = '_' * (bar_len - len(hashes))
        sys.stdout.write("\r%s %s %d%%  %s" % (str(num_run), hashes + spaces, int(num_run / num_total * 100),
                                               file_name))
        sys.stdout.flush()


class MrsParser:
    def __init__(self):
        config_manager.get_mrs_config()
        self.parse_sheet = config_manager.config_mrs['parse_sheet']
        self.exception_sheet = config_manager.config_mrs['exception_sheet']
        self.head_counter = 0
        self.data_head = {}
        self.data_data = {}

    def get_parser_sheet(self, tree):
        """获取需处理表格"""
        if self.parse_sheet == ['']:
            self.parse_sheet = [q.attrib['mrName'] for q in tree.iter(tag='measurement')]
        self.parse_sheet = [r for r in self.parse_sheet if r not in self.exception_sheet]

    def get_parser_head(self, tree):
        for s in tree.iter('measurement'):
            if s.attrib['mrName'] in self.parse_sheet:
                if s.attrib['mrName'] not in self.data_head:
                    for t in s:
                        if t.tag == 'smr':
                            self.data_head[s.attrib['mrName']] = t.text.split(' ')

    def parser(self, tree):
        # 获取处理表格及表头，仅允许一次
        if self.head_counter == 0:
            self.get_parser_sheet(tree)
            self.get_parser_head(tree)
            self.head_counter = 1

        # 解码主程序
        # 判断汇总类型
        report_time = 'sum'
        if 'hour' in config_manager.config_mrs['gather_type']:
            for temp_file_header in tree.iter(tag='fileHeader'):
                report_time = temp_file_header.attrib['reportTime'][:temp_file_header.attrib['reportTime'].find(':')]
                break

        for u in tree.iter('measurement'):
            if u.attrib['mrName'] in self.parse_sheet:
                if u.attrib['mrName'] not in self.data_data:
                    self.data_data[u.attrib['mrName']] = {}
                if report_time not in self.data_data[u.attrib['mrName']]:
                    self.data_data[u.attrib['mrName']][report_time] = {}
                for j in u:
                    if j.tag == 'smr':
                        pass
                    else:
                        for k in j:
                            if j.attrib['id'] in self.data_data[u.attrib['mrName']][report_time]:
                                self.data_data[u.attrib['mrName']][report_time][j.attrib['id']] += numpy.array(list(
                                    map(int, k.text.rstrip().split(' '))))
                            else:
                                self.data_data[u.attrib['mrName']][report_time][j.attrib['id']] = numpy.array(list(map(
                                    int, k.text.rstrip().split(' '))))

    def parser_plus(self):
        data_data_plus = {}
        for y in self.data_data:
            if y not in data_data_plus:
                data_data_plus[y] = {'sum': {}}
            for j in self.data_data[y]:
                for k in self.data_data[y][j]:
                    try:
                        data_data_plus[y]['sum'][k] += self.data_data[y][j][k]
                    except:
                        data_data_plus[y]['sum'][k] = self.data_data[y][j][k]
                    self.data_data[y][j][k] = ''
        self.data_data = data_data_plus

    def data_writer(self, gather_type):
        config_main = config_manager.config_main
        config_manager.makedir()
        for i in self.data_data:
            if config_main['timing'][0] == '1':
                if gather_type == 'hour':
                    f = open(''.join((config_main['target_path'][0], '\\', i, '_', config_manager.yesterday,
                                      '_hour.csv')), 'w')
                elif gather_type == 'id':
                    f = open(''.join((config_main['target_path'][0], '\\', i, '_', config_manager.yesterday, '.csv')),
                             'w')
            else:
                if gather_type == 'hour':
                    f = open(''.join((config_main['target_path'][0], '\\', i, '_hour.csv')), 'w')
                elif gather_type == 'id':
                    f = open(''.join((config_main['target_path'][0], '\\', i, '.csv')), 'w')

            if config_main['timing'][0] == '1':
                f.write('day,time,ECID,ENB_ID,ENB_CELLID,')
            else:
                f.write('time,ECID,ENB_ID,ENB_CELLID,')

            if i == 'MR.RSRP':
                f.write('MR覆盖率(RSRP>=-110),RSRP>=-110计数器,all计数器,')
            for j in self.data_head[i]:
                f.write(j)
                f.write(',')
            f.write('\n')

            for j in self.data_data[i]:
                for k in self.data_data[i][j]:

                    if config_main['timing'][0] == '1':
                        f.write(config_manager.yesterday)
                        f.write(',')

                    f.write(j + ',' + k + ',')
                    try:
                        f.write(str(int(k) // 256))
                        f.write(',')
                        f.write(''.join((str(int(k) // 256), '_', str(int(k) % 256))))
                        f.write(',')
                    except:
                        f.write(str(int(k[:k.find(':')]) // 256))
                        f.write(',')
                        f.write(''.join((str(int(k[:k.find(':')]) // 256), '_', str(int(k[:k.find(':')]) % 256))))
                        f.write(',')
                    if i == 'MR.RSRP':
                        denominator = numpy.sum(self.data_data[i][j][k])
                        numerator = numpy.sum(self.data_data[i][j][k][7:])
                        if denominator == 0:
                            f.write('-')
                        else:
                            f.write(str(round(numerator / denominator * 100, 2)))
                        f.write(''.join((',', str(numerator), ',', str(denominator), ',')))
                    f.write(','.join(list(map(str, self.data_data[i][j][k]))))
                    f.write('\n')


class MroParser:
    def __init__(self):
        config_manager.get_mro_config()
        self.head_counter = 0
        self.data_head = ''
        self.data_data = {}

        # 临时变量
        self.l_list = [0]

    def get_parser_head(self, tree):
        for ii in tree.iter('measurement'):
            for jj in ii:
                if jj.text[:12] == 'MR.LteScRSRP':
                    self.data_head = jj.text.split(' ')

    def overlap(self, k, overlap_num, overlap, overlap_ncell):
        overlap_db = int(overlap[0])
        overlap_ncell_rsrp = int(overlap_ncell[0])
        id_temp = k.attrib['id']
        if (self.l_list[11] != 0) and (
                    self.l_list[9] >= (140 + overlap_ncell_rsrp)) and (
                    (self.l_list[9] - self.l_list[0]) >= overlap_db) and (
                    self.l_list[7] == self.l_list[11]):
            # 采样点
            if overlap_num == 0:
                temp_overlap_ncell_rsrp = '_'.join(('overlap', str(overlap_db), str(overlap_ncell_rsrp)))
                try:
                    self.data_data[id_temp][temp_overlap_ncell_rsrp] += 1
                except:
                    self.data_data[id_temp][temp_overlap_ncell_rsrp] = 1
                overlap_num = 1

            temp_s_cell_rsrp = '_'.join(('overlap', str(overlap_db), str(overlap_ncell_rsrp), 's_cell_rsrp'))
            temp_n_cell_rsrp = '_'.join(('overlap', str(overlap_db), str(overlap_ncell_rsrp), 'n_cell_rsrp'))
            temp_sctadv = '_'.join(('overlap', str(overlap_db), str(overlap_ncell_rsrp), 'ScTadv'))
            try:
                self.data_data[id_temp][temp_s_cell_rsrp] += self.l_list[0]
                self.data_data[id_temp][temp_s_cell_rsrp] //= 2

                self.data_data[id_temp][temp_n_cell_rsrp] += self.l_list[9]
                self.data_data[id_temp][temp_n_cell_rsrp] //= 2

                self.data_data[id_temp][temp_sctadv] += self.l_list[2]
                self.data_data[id_temp][temp_sctadv] //= 2
            except:
                self.data_data[id_temp][temp_s_cell_rsrp] = self.l_list[0]
                self.data_data[id_temp][temp_n_cell_rsrp] = self.l_list[9]
                self.data_data[id_temp][temp_sctadv] = self.l_list[2]
        return overlap_num

    def ecid_ecid(self, k, n_cell_earfcn_pci):

        def fun_minus11():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['<-10db'] += 1

        def fun_minus10():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-10db'] += 1

        def fun_minus9():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-9db'] += 1

        def fun_minus8():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-8db'] += 1

        def fun_minus7():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-7db'] += 1

        def fun_minus6():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-6db'] += 1

        def fun_minus5():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-5db'] += 1

        def fun_minus4():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-4db'] += 1

        def fun_minus3():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-3db'] += 1

        def fun_minus2():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-2db'] += 1

        def fun_minus1():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['-1db'] += 1

        def fun_0():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['0db'] += 1

        def fun_1():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['1db'] += 1

        def fun_2():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['2db'] += 1

        def fun_3():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['3db'] += 1

        def fun_4():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['4db'] += 1

        def fun_5():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['5db'] += 1

        def fun_6():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['6db'] += 1

        def fun_7():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['7db'] += 1

        def fun_8():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['8db'] += 1

        def fun_9():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['9db'] += 1

        def fun_10():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['10db'] += 1

        def fun_11():
            self.data_data[k.attrib['id']]['ECID_ECID'][n_cell_earfcn_pci]['>10db'] += 1

        fun_list = {-11: fun_minus11, -10: fun_minus10, -9: fun_minus9,
                    -8: fun_minus8, -7: fun_minus7, -6: fun_minus6,
                    -5: fun_minus5, -4: fun_minus4, -3: fun_minus3, -2: fun_minus2,
                    -1: fun_minus1, 0: fun_0, 1: fun_1,
                    2: fun_2, 3: fun_3, 4: fun_4, 5: fun_5, 6: fun_6, 7: fun_7,
                    8: fun_8, 9: fun_9, 10: fun_10, 11: fun_11}

        a = self.l_list[9] - self.l_list[0]
        try:
            fun_list[a]()
        except:
            if a < -10:
                a = -11
            elif a > 10:
                a = 11
            fun_list[a]()

    def earfcn_pci_cellid_relate(self):

        """读取基础数据，建立earfcn pci cellid 索引表"""

        f = open(os.path.join(config_manager.main_path, 'enb_basedat.csv'), encoding='utf-8-sig')
        basedatas = [i.strip().split(',') for i in f.readlines()]

        self.earfcn_pci_cellid = {}
        self.enbid_list = {}

        for k in basedatas:
            if k[7] not in self.earfcn_pci_cellid:
                self.earfcn_pci_cellid[k[7]] = {}
            if k[6] not in self.earfcn_pci_cellid[k[7]]:
                self.earfcn_pci_cellid[k[7]][k[6]] = []
            try:
                self.earfcn_pci_cellid[k[7]][k[6]].append((k[0], k[1], float(k[4]), float(k[5])))
            except:
                pass

            if k[0] not in self.enbid_list:
                try:
                    self.enbid_list[k[0]] = (float(k[4]), float(k[5]))
                except:
                    pass

    @staticmethod
    def distance(lon_1, lat_1, lon_2, lat_2):

        """计算距离"""

        lon1, lat1, lon2, lat2 = map(math.radians, [lon_1, lat_1, lon_2, lat_2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371  # 地球平均半径，单位为公里
        return c * r * 1000

    def min_distance_cell(self, s_enbid, n_earfcn, n_pci):

        """提取距离最近的小区"""

        min_distance_list = {}
        min_cell = '-'
        min_stance = '-'
        try:
            for i in self.earfcn_pci_cellid[n_earfcn][n_pci]:
                min_distance_list[self.distance(self.enbid_list[s_enbid][0], self.enbid_list[s_enbid][1], i[2],
                                                i[3])] = i[1]
                min_stance = min(min_distance_list)
                min_cell = min_distance_list[min_stance]
                min_stance = int(min_stance)
        except:
            pass

        return min_cell, min_stance

    @staticmethod
    def rsrp_region(rsrp):
        rsrp_region_dict = {
            '19': 'RSRP_00', '20': 'RSRP_01', '21': 'RSRP_01', '22': 'RSRP_01', '23': 'RSRP_01', '24': 'RSRP_01',
            '25': 'RSRP_02', '26': 'RSRP_03', '27': 'RSRP_04', '28': 'RSRP_05', '29': 'RSRP_06', '30': 'RSRP_07',
            '31': 'RSRP_08', '32': 'RSRP_09', '33': 'RSRP_10', '34': 'RSRP_11', '35': 'RSRP_12', '36': 'RSRP_13',
            '37': 'RSRP_14', '38': 'RSRP_15', '39': 'RSRP_16', '40': 'RSRP_17', '41': 'RSRP_18', '42': 'RSRP_19',
            '43': 'RSRP_20', '44': 'RSRP_21', '45': 'RSRP_22', '46': 'RSRP_23', '47': 'RSRP_24', '48': 'RSRP_25',
            '49': 'RSRP_26', '50': 'RSRP_27', '51': 'RSRP_28', '52': 'RSRP_29', '53': 'RSRP_30', '54': 'RSRP_31',
            '55': 'RSRP_32', '56': 'RSRP_33', '57': 'RSRP_34', '58': 'RSRP_35', '59': 'RSRP_36', '60': 'RSRP_37',
            '61': 'RSRP_37', '62': 'RSRP_38', '63': 'RSRP_38', '64': 'RSRP_39', '65': 'RSRP_39', '66': 'RSRP_40',
            '67': 'RSRP_40', '68': 'RSRP_41', '69': 'RSRP_41', '70': 'RSRP_42', '71': 'RSRP_42', '72': 'RSRP_43',
            '73': 'RSRP_43', '74': 'RSRP_44', '75': 'RSRP_44', '76': 'RSRP_45', '77': 'RSRP_45', '78': 'RSRP_46',
            '79': 'RSRP_46', '80': 'RSRP_47'
        }
        rsrp_temp = int(rsrp)
        if 19 <= rsrp_temp < 80:
            rsrp_temp = str(rsrp_temp)
        elif rsrp_temp < 19:
            rsrp_temp = '19'
        elif rsrp_temp >= 80:
            rsrp_temp = '80'
        return rsrp_region_dict[rsrp_temp]

    def parser(self, tree):
        # 获取重叠覆盖率门限并重命名
        overlap_db_1 = config_manager.config_mro['overlap_db_1']
        overlap_ncell_rsrp_1 = config_manager.config_mro['overlap_ncell_rsrp_1']
        overlap_db_2 = config_manager.config_mro['overlap_db_2']
        overlap_ncell_rsrp_2 = config_manager.config_mro['overlap_ncell_rsrp_2']

        # 获取表头，仅允许一次
        if self.head_counter == 0:
            self.get_parser_head(tree)
            self.head_counter = 1

        # 解码主程序
        # MRO共有三个measurement，但是均未命名，因此需先判断哪个measurement是所需要提取的数据
        root = tree.getroot()
        measurement_len = len([i for i in tree.iter('measurement')])
        for measurement_len_ob in range(measurement_len):
            if (root[1][measurement_len_ob][0].tag == 'smr') and (
                        root[1][measurement_len_ob][0].text[:12] == 'MR.LteScRSRP'):
                for k in root[1][measurement_len_ob].iter('object'):
                    id_temp = k.attrib['id']
                    if id_temp not in self.data_data:
                        self.data_data[id_temp] = {
                            's_samplint': 0,
                            'ECID_ECID': {},
                            'earfcn': {'samplint': {}, 's_full_samplint': {}}
                        }
                    l_num = 0
                    overlap_num_1 = 0
                    overlap_num_2 = 0
                    rsrp_earfcn_dict = {}
                    for l in k:
                        self.l_list = numpy.array(list(map(int, l.text.replace('NIL', '0').rstrip().split(' '))))
                        # 计算主小区采样点及采样信息
                        if l_num == 0:
                            self.data_data[id_temp]['s_samplint'] += 1
                            try:
                                self.data_data[id_temp]['s_basic'] += numpy.concatenate((
                                    self.l_list[0:9], self.l_list[20:22], self.l_list[23:27]))
                                self.data_data[id_temp]['s_basic'] /= 2
                            except:
                                self.data_data[id_temp]['s_basic'] = numpy.concatenate(
                                    (self.l_list[0:9], self.l_list[20:22], self.l_list[23:27]))

                            # 统计主小区全量的移动测量数据
                            rsrp_tmp = self.rsrp_region(self.l_list[0])
                            try:
                                self.data_data[id_temp]['earfcn']['s_full_samplint'][rsrp_tmp] += 1
                            except:
                                self.data_data[id_temp]['earfcn']['s_full_samplint'][rsrp_tmp] = 1

                            l_num = 1

                        # 计算重叠覆盖率_1
                        if overlap_db_1 != '' and overlap_ncell_rsrp_1 != '':
                            overlap_num_1 = self.overlap(k, overlap_num_1, overlap_db_1, overlap_ncell_rsrp_1)

                        # 计算重叠覆盖率_2
                        if overlap_db_2 != '' and overlap_ncell_rsrp_2 != '':
                            overlap_num_2 = self.overlap(k, overlap_num_2, overlap_db_2, overlap_ncell_rsrp_2)

                        # 计算与邻区关系ECID 及 频点关系
                        n_earfcn = self.l_list[11]
                        if n_earfcn != 0:

                            n_cell_earfcn_pci = '_'.join(list(map(str, self.l_list[11:13])))

                            # 与邻区关系ECID

                            if n_cell_earfcn_pci not in self.data_data[id_temp]['ECID_ECID']:
                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci] = {
                                    'ncrsrp': self.l_list[9], 'scrsrp': self.l_list[0], 'ScTadv': self.l_list[2],
                                    'n_samplint': 0, '<-10db': 0, '-10db': 0, '-9db': 0, '-8db': 0, '-7db': 0,
                                    '-6db': 0, '-5db': 0, '-4db': 0, '-3db': 0, '-2db': 0, '-1db': 0, '0db': 0,
                                    '1db': 0, '2db': 0, '3db': 0, '4db': 0, '5db': 0, '6db': 0, '7db': 0,
                                    '8db': 0, '9db': 0, '10db': 0, '>10db': 0}
                                self.ecid_ecid(k, n_cell_earfcn_pci)
                            else:
                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci][
                                    'ncrsrp'] += self.l_list[9]
                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['ncrsrp'] /= 2

                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['scrsrp'] += self.l_list[0]
                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['scrsrp'] /= 2

                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['ScTadv'] += self.l_list[2]
                                self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['ScTadv'] /= 2
                                self.ecid_ecid(k, n_cell_earfcn_pci)
                            # 统计邻区采样点
                            self.data_data[id_temp]['ECID_ECID'][n_cell_earfcn_pci]['n_samplint'] += 1

                            # 计算非全量的移动测量数据
                            try:
                                rsrp_earfcn_dict[n_earfcn][0].append(int(self.l_list[9]))
                            except:
                                rsrp_earfcn_dict[n_earfcn] = [[int(self.l_list[9])], self.l_list[0]]

                    # 汇总计算非全量的移动测量数据
                    if len(rsrp_earfcn_dict) != 0:
                        for i in rsrp_earfcn_dict.keys():
                            rsrp_region_temp_n = self.rsrp_region(str(max(rsrp_earfcn_dict[i][0])))
                            rsrp_region_temp_s = self.rsrp_region(str(rsrp_earfcn_dict[i][1]))
                            try:
                                self.data_data[id_temp]['earfcn']['samplint'][i][0][rsrp_region_temp_n] += 1
                            except:
                                try:
                                    self.data_data[id_temp]['earfcn']['samplint'][i][0][rsrp_region_temp_n] = 1
                                except:
                                    self.data_data[id_temp]['earfcn']['samplint'][i] = [{rsrp_region_temp_n: 1},
                                                                                        {}]
                            try:
                                self.data_data[id_temp]['earfcn']['samplint'][i][1][rsrp_region_temp_s] += 1
                            except:
                                self.data_data[id_temp]['earfcn']['samplint'][i][1][rsrp_region_temp_s] = 1

                # 获取到所需要的measurement后，则退出循环
                break

    def writer(self):
        # 重命名counter名称（太长了不好看）
        overlap_1_s = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_1'][0]),
                                str(config_manager.config_mro['overlap_ncell_rsrp_1'][0]), 's_cell_rsrp'))
        overlap_1_n = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_1'][0]),
                                str(config_manager.config_mro['overlap_ncell_rsrp_1'][0]), 'n_cell_rsrp'))
        overlap_1_ta = '_'.join(
            ('overlap', str(config_manager.config_mro['overlap_db_1'][0]), str(config_manager.config_mro[
                                                                                   'overlap_ncell_rsrp_1'][0]),
             'ScTadv'))
        overlap_1_samplint = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_1'][0]),
                                       str(config_manager.config_mro['overlap_ncell_rsrp_1'][0]), 'samplint'))
        overlap_2_samplint = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_2'][0]),
                                       str(config_manager.config_mro['overlap_ncell_rsrp_2'][0]), 'samplint'))
        overlap_2_s = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_2'][0]),
                                str(config_manager.config_mro['overlap_ncell_rsrp_2'][0]), 's_cell_rsrp'))
        overlap_2_n = '_'.join(('overlap', str(config_manager.config_mro['overlap_db_2'][0]),
                                str(config_manager.config_mro['overlap_ncell_rsrp_2'][0]), 'n_cell_rsrp'))
        overlap_2_ta = '_'.join(
            ('overlap', str(config_manager.config_mro['overlap_db_2'][0]), str(config_manager.config_mro[
                                                                                   'overlap_ncell_rsrp_2'][0]),
             'ScTadv'))
        # 与解码结果保持一致
        overlap_1_sam = '_'.join(
            ('overlap', str(config_manager.config_mro['overlap_db_1'][0]), str(config_manager.config_mro[
                                                                                   'overlap_ncell_rsrp_1'][0])))
        overlap_2_sam = '_'.join(
            ('overlap', str(config_manager.config_mro['overlap_db_2'][0]), str(config_manager.config_mro[
                                                                                   'overlap_ncell_rsrp_2'][0])))

        # 生成文件
        mro_main_f = open(''.join((config_manager.config_main['target_path'][0], '\\', 'MRO_main.csv')), 'w')
        # 写入表头
        mro_main_f.write(
            'ECID,ENB_CELL,MR.LteScRSRP,MR.LteScRSRQ,MR.LteScTadv,MR.LteSceNBRxTxTimeDiff,MR.LteScPHR,MR.LteScAOA,'
            'MR.LteScSinrUL,MR.LteScEarfcn,MR.LteScPci,MR.LteScPUSCHPRBNum,MR.LteScPDSCHPRBNum,MR.LteScRI1,'
            'MR.LteScRI2,MR.LteScRI4,MR.LteScRI8,s_samplint,')
        mro_main_f.write(','.join((overlap_1_samplint, overlap_1_s, overlap_1_n, overlap_1_ta, overlap_2_samplint,
                                   overlap_2_s, overlap_2_n, overlap_2_ta, '\n')))
        # 写入解码结果
        for a in self.data_data:
            mro_main_f.write(a)
            mro_main_f.write(',')
            mro_main_f.write('_'.join((str(int(a) // 256), str(int(a) % 256))))
            mro_main_f.write(',')
            mro_main_f.write(
                ','.join(list(map(str, self.data_data[a]['s_basic']))))
            mro_main_f.write(',')
            mro_main_f.write(str(self.data_data[a]['s_samplint']))
            mro_main_f.write(',')
            for temp_b in (overlap_1_sam, overlap_1_s, overlap_1_n, overlap_1_ta,
                           overlap_2_sam, overlap_2_s, overlap_2_n, overlap_2_ta):
                try:
                    if temp_b in (overlap_1_s, overlap_1_n, overlap_2_s, overlap_2_n):
                        mro_main_f.write(str(self.data_data[a][temp_b] - 140))
                    else:
                        mro_main_f.write(str(self.data_data[a][temp_b]))
                except:
                    mro_main_f.write('-')
                mro_main_f.write(',')
            mro_main_f.write('\n')

        # 读取小区基础数据文件
        self.earfcn_pci_cellid_relate()

        # 生成结果文件ECID
        # 生成文件
        mro_main_f = open(''.join((config_manager.config_main['target_path'][0], '\\', 'MRO_ECID.csv')), 'w')
        # 写入表头
        mro_main_f.write('s_ECID')
        mro_main_f.write(',ENB_CELL,s_earfcn,s_pci,n_ENB_CELL,')
        mro_main_f.write('n_earfcn,n_pci,s_n_distance(m)')
        mro_main_f.write(',Scrsrp,ScTadv,n_samplint,Ncrsrp,<-10db,-10db,-9db,-8db,-7db,-6db,-5db,-4db,-3db,-2db,-1db,'
                         '0db,1db,2db,3db,4db,5db,6db,7db,8db,9db,10db,>10db\n')
        # 写入结果文件
        for c in self.data_data:
            for d in self.data_data[c]['ECID_ECID']:
                mro_main_f.write(c)
                mro_main_f.write(',')
                mro_main_f.write('_'.join((str(int(c) // 256), str(int(c) % 256))))
                mro_main_f.write(',')
                mro_main_f.write(str(self.data_data[c]['s_basic'][7]))
                mro_main_f.write(',')
                mro_main_f.write(str(self.data_data[c]['s_basic'][8]))
                mro_main_f.write(',')

                earfcn_pci = d.split('_')
                min_cell, min_stance = self.min_distance_cell(str(int(c) // 256), earfcn_pci[0], earfcn_pci[1])
                mro_main_f.write(min_cell)

                mro_main_f.write(',')
                mro_main_f.write(','.join(earfcn_pci))
                mro_main_f.write(',')
                mro_main_f.write(str(min_stance))
                mro_main_f.write(',')
                for e in (
                        'scrsrp', 'ScTadv', 'n_samplint', 'ncrsrp', '<-10db',
                        '-10db', '-9db', '-8db', '-7db', '-6db', '-5db',
                        '-4db', '-3db', '-2db', '-1db', '0db', '1db', '2db',
                        '3db', '4db', '5db', '6db', '7db', '8db', '9db',
                        '10db', '>10db'):
                    mro_main_f.write(str(self.data_data[c]['ECID_ECID'][d][e]))
                    mro_main_f.write(',')
                mro_main_f.write('\n')

        with open(''.join((config_manager.config_main['target_path'][0], '\\', 'MRO_EARFCN.csv')), 'w') as mro_earfcn_f:
            # 写入表头
            head_list_full_s = ['s_FULL_RSRP_00', 's_FULL_RSRP_01', 's_FULL_RSRP_02', 's_FULL_RSRP_03',
                                's_FULL_RSRP_04', 's_FULL_RSRP_05', 's_FULL_RSRP_06', 's_FULL_RSRP_07',
                                's_FULL_RSRP_08', 's_FULL_RSRP_09', 's_FULL_RSRP_10', 's_FULL_RSRP_11',
                                's_FULL_RSRP_12', 's_FULL_RSRP_13', 's_FULL_RSRP_14', 's_FULL_RSRP_15',
                                's_FULL_RSRP_16', 's_FULL_RSRP_17', 's_FULL_RSRP_18', 's_FULL_RSRP_19',
                                's_FULL_RSRP_20', 's_FULL_RSRP_21', 's_FULL_RSRP_22', 's_FULL_RSRP_23',
                                's_FULL_RSRP_24', 's_FULL_RSRP_25', 's_FULL_RSRP_26', 's_FULL_RSRP_27',
                                's_FULL_RSRP_28', 's_FULL_RSRP_29', 's_FULL_RSRP_30', 's_FULL_RSRP_31',
                                's_FULL_RSRP_32', 's_FULL_RSRP_33', 's_FULL_RSRP_34', 's_FULL_RSRP_35',
                                's_FULL_RSRP_36', 's_FULL_RSRP_37', 's_FULL_RSRP_38', 's_FULL_RSRP_39',
                                's_FULL_RSRP_40', 's_FULL_RSRP_41', 's_FULL_RSRP_42', 's_FULL_RSRP_43',
                                's_FULL_RSRP_44', 's_FULL_RSRP_45', 's_FULL_RSRP_46', 's_FULL_RSRP_47']
            head_list_s = ['s_RSRP_00', 's_RSRP_01', 's_RSRP_02', 's_RSRP_03', 's_RSRP_04', 's_RSRP_05', 's_RSRP_06',
                           's_RSRP_07', 's_RSRP_08', 's_RSRP_09', 's_RSRP_10', 's_RSRP_11', 's_RSRP_12', 's_RSRP_13',
                           's_RSRP_14', 's_RSRP_15', 's_RSRP_16', 's_RSRP_17', 's_RSRP_18', 's_RSRP_19', 's_RSRP_20',
                           's_RSRP_21', 's_RSRP_22', 's_RSRP_23', 's_RSRP_24', 's_RSRP_25', 's_RSRP_26', 's_RSRP_27',
                           's_RSRP_28', 's_RSRP_29', 's_RSRP_30', 's_RSRP_31', 's_RSRP_32', 's_RSRP_33', 's_RSRP_34',
                           's_RSRP_35', 's_RSRP_36', 's_RSRP_37', 's_RSRP_38', 's_RSRP_39', 's_RSRP_40', 's_RSRP_41',
                           's_RSRP_42', 's_RSRP_43', 's_RSRP_44', 's_RSRP_45', 's_RSRP_46', 's_RSRP_47']
            head_list_n = ['n_RSRP_00', 'n_RSRP_01', 'n_RSRP_02', 'n_RSRP_03', 'n_RSRP_04', 'n_RSRP_05', 'n_RSRP_06',
                           'n_RSRP_07', 'n_RSRP_08', 'n_RSRP_09', 'n_RSRP_10', 'n_RSRP_11', 'n_RSRP_12', 'n_RSRP_13',
                           'n_RSRP_14', 'n_RSRP_15', 'n_RSRP_16', 'n_RSRP_17', 'n_RSRP_18', 'n_RSRP_19', 'n_RSRP_20',
                           'n_RSRP_21', 'n_RSRP_22', 'n_RSRP_23', 'n_RSRP_24', 'n_RSRP_25', 'n_RSRP_26', 'n_RSRP_27',
                           'n_RSRP_28', 'n_RSRP_29', 'n_RSRP_30', 'n_RSRP_31', 'n_RSRP_32', 'n_RSRP_33', 'n_RSRP_34',
                           'n_RSRP_35', 'n_RSRP_36', 'n_RSRP_37', 'n_RSRP_38', 'n_RSRP_39', 'n_RSRP_40', 'n_RSRP_41',
                           'n_RSRP_42', 'n_RSRP_43', 'n_RSRP_44', 'n_RSRP_45', 'n_RSRP_46', 'n_RSRP_47']
            mro_earfcn_f.write(','.join(('ECID', 'ENB_CELL', 'n_EARFCN')))
            mro_earfcn_f.write(',')
            mro_earfcn_f.write(','.join(head_list_full_s))
            mro_earfcn_f.write(',')
            mro_earfcn_f.write(','.join(head_list_s))
            mro_earfcn_f.write(',')
            mro_earfcn_f.write(','.join(head_list_n))
            mro_earfcn_f.write('\n')
            head_list_all = ['RSRP_00', 'RSRP_01', 'RSRP_02', 'RSRP_03', 'RSRP_04', 'RSRP_05', 'RSRP_06',
                             'RSRP_07', 'RSRP_08', 'RSRP_09', 'RSRP_10', 'RSRP_11', 'RSRP_12', 'RSRP_13',
                             'RSRP_14', 'RSRP_15', 'RSRP_16', 'RSRP_17', 'RSRP_18', 'RSRP_19', 'RSRP_20',
                             'RSRP_21', 'RSRP_22', 'RSRP_23', 'RSRP_24', 'RSRP_25', 'RSRP_26', 'RSRP_27',
                             'RSRP_28', 'RSRP_29', 'RSRP_30', 'RSRP_31', 'RSRP_32', 'RSRP_33', 'RSRP_34',
                             'RSRP_35', 'RSRP_36', 'RSRP_37', 'RSRP_38', 'RSRP_39', 'RSRP_40', 'RSRP_41',
                             'RSRP_42', 'RSRP_43', 'RSRP_44', 'RSRP_45', 'RSRP_46', 'RSRP_47']
            # 写入结果
            for id_temp in self.data_data:
                enb_cellid = '_'.join((str(int(id_temp) // 256), str(int(id_temp) % 256)))
                for earfcn_temp in self.data_data[id_temp]['earfcn']['samplint']:
                    mro_earfcn_f.write(id_temp)
                    mro_earfcn_f.write(',')
                    mro_earfcn_f.write(enb_cellid)
                    mro_earfcn_f.write(',')
                    mro_earfcn_f.write(str(earfcn_temp))
                    mro_earfcn_f.write(',')
                    for head_list_temp in head_list_all:
                        try:
                            mro_earfcn_f.write(str(self.data_data[id_temp]['earfcn']['s_full_samplint'][
                                                       head_list_temp]))
                        except:
                            mro_earfcn_f.write('0')
                        mro_earfcn_f.write(',')

                    for head_list_temp in head_list_all:
                        try:
                            mro_earfcn_f.write(
                                str(self.data_data[id_temp]['earfcn']['samplint'][earfcn_temp][1][head_list_temp]))
                        except:
                            mro_earfcn_f.write('0')
                        mro_earfcn_f.write(',')

                    for head_list_temp in head_list_all:
                        try:
                            mro_earfcn_f.write(
                                str(self.data_data[id_temp]['earfcn']['samplint'][earfcn_temp][0][head_list_temp]))
                        except:
                            mro_earfcn_f.write('0')
                        mro_earfcn_f.write(',')

                    mro_earfcn_f.write('\n')


if __name__ == '__main__':
    star_time = time.time()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    # 初始化配置
    config_manager = ConfigManager()

    parse_type = config_manager.config_main['parse_type']

    # 统计获取到的文件数：
    for temp_i in config_manager.parse_file_list:
        num = 0
        for temp_k in config_manager.parse_file_list[temp_i]:
            num += len(config_manager.parse_file_list[temp_i][temp_k])
        print('>>> 获取到 ', temp_i, ' 文件:', num)

    # 分别启动解码类型的解码程序
    parse_type_choice = {'mrs': config_manager.star_mrs_manager,
                         'mro': config_manager.star_mro_manager
                         }
    for b in parse_type:
        if b in config_manager.parse_file_list:
            parse_type_choice[b.lower()]()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
