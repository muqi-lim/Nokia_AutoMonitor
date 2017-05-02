import xml.etree.ElementTree as ET
import numpy
import configparser
import os
import sys
import datetime
import gzip
import tarfile
import time
import multiprocessing
# import copy
import csv
import traceback
import math
import shutil


def copy_right():
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

    2017-3-22 重构，改进算法，支持多进程处理，效率大幅提高；
    2017-4-18 添加过滤器，支持指定需解码基站数据，并提取对应原始数据到指定文件夹；
    2017-4-19 添加 mro_rsrp 表，记录的为MR覆盖率各个RSRP等级分布情况，同mrs解码的MR.RSRP表；
    2017-4-20 过滤器增加时段字段，可以指定需解码的时段；
    2017-4-23 添加进度条；
    2017-4-24 MRO覆盖率基准值由140调整为141（调整后与MRS解码的覆盖率一致）；
    2017-4-24 实现按小时级汇总；
    2017-5-2 增加解码表类型 mro_ecid_hour，此表为mro_ecid的小时级表；

    ''')
    print('-' * 36)
    print('      >>>   starting   <<<')
    print('-' * 36)
    print('\n')
    time.sleep(1)


################################################################################
# Module multiprocessing is organized differently in Python 3.4+
try:
    # Python 3.4+
    if sys.platform.startswith('win'):
        import multiprocessing.popen_spawn_win32 as forking
    else:
        import multiprocessing.popen_fork as forking
except ImportError:
    import multiprocessing.forking as forking

if sys.platform.startswith('win'):
    # First define a modified version of Popen.
    class _Popen(forking.Popen):
        def __init__(self, *args, **kw):
            if hasattr(sys, 'frozen'):
                # We have to set original _MEIPASS2 value from sys._MEIPASS
                # to get --onefile mode working.
                os.putenv('_MEIPASS2', sys._MEIPASS)
            try:
                super(_Popen, self).__init__(*args, **kw)
            finally:
                if hasattr(sys, 'frozen'):
                    # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                    # available. In those cases we cannot delete the variable
                    # but only set it to the empty string. The bootloader
                    # can handle this case.
                    if hasattr(os, 'unsetenv'):
                        os.unsetenv('_MEIPASS2')
                    else:
                        os.putenv('_MEIPASS2', '')
    # Second override 'Popen' class with our modified version.
    forking.Popen = _Popen


################################################################################


class Main:
    def __init__(self):
        """初始化"""
        copy_right()
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((self.main_path, '\\', 'config.ini')), encoding='utf-8-SIG')
        # 初始化配置列表
        self.config_main = {}
        self.config_mrs = {}
        self.config_mro = {}
        self.config_filter = {}

        self.yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

        # 获取 main 配置
        self.get_main_config()

        # 生成结果一个空字典
        self.value_lists = {'mrs': {}, 'mro': []}

        # 统计文件数
        self.all_num = {'mrs': 0, 'mro': 0, 'mre': 0}

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

        # process设置
        if self.config_main['process'][0] == 'min':
            self.config_main['process'][0] = '2'
        elif self.config_main['process'][0] == 'mid':
            self.config_main['process'][0] = int(multiprocessing.cpu_count() / 2 + 1)
        elif self.config_main['process'][0] in ['max', '', '0']:
            self.config_main['process'][0] = int(multiprocessing.cpu_count())
        if self.config_main['process'][0] in ['1', 1]:
            self.config_main['process'][0] = '2'

        """获取 filter 配置文件"""
        for a in self.cf.options('filter'):
            self.config_filter[a] = self.cf.get('filter', a).split(',')
        self.config_filter['filter_type'] = [i.lower() for i in self.config_filter['filter_type']]
        if self.config_filter['filter_id'] == [''] and self.config_filter['filter_type'] == ['']:
            self.config_filter['active_filter'] = 0

        # 获取处理列表
        self.parse_file_list = {}
        self.get_files(self.config_main['source_path'][0])

        # 检测结果目录是否存在，如果不存在则创建
        if len(self.parse_file_list) != 0:
            if not os.path.isdir(self.config_main['target_path'][0]):
                os.makedirs(self.config_main['target_path'][0])

    def get_config(self, mr_type):

        """获取配置"""

        self.temp_mrs_data = {}
        self.temp_mro_data = {}
        # self.mro_data_data = {}
        if mr_type == 'mrs':
            for l in self.cf.options('MRS'):
                self.config_mrs[l] = self.cf.get('MRS', l).split(',')
            self.mrs_parse_sheet = []
            self.mrs_head = {}
        elif mr_type == 'mro':
            for n in self.cf.options('MRO'):
                self.config_mro[n] = self.cf.get('MRO', n).split(',')
            # RSRP对应表，用于MR覆盖率
            if self.config_mro['mro_rsrp_standard'] == ['140']:
                self.mro_rsrp_list = {
                    140: 47, 139: 47, 138: 47, 137: 47, 136: 47, 135: 47, 134: 47, 133: 47, 132: 47, 131: 47, 130: 47,
                    129: 47, 128: 47, 127: 47, 126: 47, 125: 47, 124: 47, 123: 47, 122: 47, 121: 47, 120: 47, 119: 47,
                    118: 47, 117: 47, 116: 47, 115: 47, 114: 47, 113: 47, 112: 47, 111: 47, 110: 47, 109: 47, 108: 47,
                    107: 47, 106: 47, 105: 47, 104: 47, 103: 47, 102: 47, 101: 47, 100: 47, 99: 47, 98: 47, 97: 47,
                    96: 47, 95: 47, 94: 47, 93: 47, 92: 47, 91: 47, 90: 47, 89: 47, 88: 47, 87: 47, 86: 47, 85: 47,
                    84: 47, 83: 47, 82: 47, 81: 47, 80: 47, 79: 46, 78: 46, 77: 45, 76: 45, 75: 44, 74: 44, 73: 43,
                    72: 43, 71: 42, 70: 42, 69: 41, 68: 41, 67: 40, 66: 40, 65: 39, 64: 39, 63: 38, 62: 38, 61: 37,
                    60: 37, 59: 36, 58: 35, 57: 34, 56: 33, 55: 32, 54: 31, 53: 30, 52: 29, 51: 28, 50: 27, 49: 26,
                    48: 25, 47: 24, 46: 23, 45: 22, 44: 21, 43: 20, 42: 19, 41: 18, 40: 17, 39: 16, 38: 15, 37: 14,
                    36: 13, 35: 12, 34: 11, 33: 10, 32: 9, 31: 8, 30: 7, 29: 6, 28: 5, 27: 4, 26: 3, 25: 2, 24: 1,
                    23: 1, 22: 1, 21: 1, 20: 1, 19: 0, 18: 0, 17: 0, 16: 0, 15: 0, 14: 0, 13: 0, 12: 0, 11: 0, 10: 0,
                    9: 0, 8: 0, 7: 0, 6: 0, 5: 0, 4: 0, 3: 0, 2: 0, 1: 0, 0: 0
                }
            elif self.config_mro['mro_rsrp_standard'] == ['141']:
                self.mro_rsrp_list = {
                    141: 47, 140: 47, 139: 47, 138: 47, 137: 47, 136: 47, 135: 47, 134: 47, 133: 47, 132: 47, 131: 47,
                    130: 47, 129: 47, 128: 47, 127: 47, 126: 47, 125: 47, 124: 47, 123: 47, 122: 47, 121: 47, 120: 47,
                    119: 47, 118: 47, 117: 47, 116: 47, 115: 47, 114: 47, 113: 47, 112: 47, 111: 47, 110: 47, 109: 47,
                    108: 47, 107: 47, 106: 47, 105: 47, 104: 47, 103: 47, 102: 47, 101: 47, 100: 47, 99: 47, 98: 47,
                    97: 47, 96: 47, 95: 47, 94: 47, 93: 47, 92: 47, 91: 47, 90: 47, 89: 47, 88: 47, 87: 47, 86: 47,
                    85: 47, 84: 47, 83: 47, 82: 47, 81: 47, 80: 46, 79: 46, 78: 45, 77: 45, 76: 44, 75: 44, 74: 43,
                    73: 43, 72: 42, 71: 42, 70: 41, 69: 41, 68: 40, 67: 40, 66: 39, 65: 39, 64: 38, 63: 38, 62: 37,
                    61: 37, 60: 36, 59: 35, 58: 34, 57: 33, 56: 32, 55: 31, 54: 30, 53: 29, 52: 28, 51: 27, 50: 26,
                    49: 25, 48: 24, 47: 23, 46: 22, 45: 21, 44: 20, 43: 19, 42: 18, 41: 17, 40: 16, 39: 15, 38: 14,
                    37: 13, 36: 12, 35: 11, 34: 10, 33: 9, 32: 8, 31: 7, 30: 6, 29: 5, 28: 4, 27: 3, 26: 2, 25: 1,
                    24: 1, 23: 1, 22: 1, 21: 1, 20: 0, 19: 0, 18: 0, 17: 0, 16: 0, 15: 0, 14: 0, 13: 0, 12: 0, 11: 0,
                    10: 0, 9: 0, 8: 0, 7: 0, 6: 0, 5: 0, 4: 0, 3: 0, 2: 0, 1: 0, 0: 0
                }

            # 计算ecid时对应表
            self.rsrp_dir = {
                -11: 4, -10: 5, -9: 6, -8: 7, -7: 8, -6: 9, -5: 10, -4: 11, -3: 12, -2: 13, -1: 14, 0: 15, 1: 16,
                2: 17, 3: 18, 4: 19, 5: 20, 6: 21, 7: 22, 8: 23, 9: 24, 10: 25, 11: 26
            }

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
        sys.stdout.write("\r%s %s %s %d%%  %s" % (str(num_run), hashes + spaces, str(num_total),
                                                             int(num_run / num_total * 100), file_name))
        sys.stdout.flush()

    def mro_earfcn_pci_cellid_relate(self):

        """读取基础数据，建立earfcn pci cellid 索引表"""

        f = open(os.path.join(config_manager.main_path, 'enb_basedat.csv'), encoding='utf-8-sig')
        basedatas = [i.strip().split(',') for i in f.readlines()]

        self.mro_earfcn_pci_cellid = {}
        self.mro_enbid_list = {}

        for k in basedatas:
            if k[7] not in self.mro_earfcn_pci_cellid:
                self.mro_earfcn_pci_cellid[k[7]] = {}
            if k[6] not in self.mro_earfcn_pci_cellid[k[7]]:
                self.mro_earfcn_pci_cellid[k[7]][k[6]] = []
            try:
                self.mro_earfcn_pci_cellid[k[7]][k[6]].append((k[0], k[1], float(k[4]), float(k[5])))
            except:
                pass

            if k[0] not in self.mro_enbid_list:
                try:
                    self.mro_enbid_list[k[0]] = (float(k[4]), float(k[5]))
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
            for i in self.mro_earfcn_pci_cellid[n_earfcn][n_pci]:
                min_distance_list[self.distance(self.mro_enbid_list[s_enbid][0], self.mro_enbid_list[s_enbid][1], i[2],
                                                i[3])] = i[1]
                min_stance = min(min_distance_list)
                min_cell = min_distance_list[min_stance]
                min_stance = int(min_stance)
        except:
            pass

        return min_cell, min_stance

    def mro_main(self, mro_object, report_time):

        """统计mro_main表"""

        temp_id = 1
        ecid = int(mro_object.attrib['id'])
        for value in mro_object.iter('v'):
            temp_value = list(map(int, value.text.rstrip().replace('NIL', '0').split(' ')))
            if temp_id == 1:
                ecid_earfcn_pci = '_'.join(map(str, (ecid, temp_value[7], temp_value[8])))
                temp_mro_main = ['mro_main',
                                 ecid_earfcn_pci,
                                 [temp_value[0],
                                  temp_value[1],
                                  temp_value[2],
                                  temp_value[4],
                                  temp_value[5],
                                  temp_value[6],
                                  temp_value[20],
                                  temp_value[21],
                                  1,
                                  0,
                                  0,
                                  0,
                                  0,
                                  0,
                                  0,
                                  0,
                                  0,
                                  0,
                                  ]
                                 ]
                # 统计MR覆盖率
                if temp_value[0] - int(self.config_mro['mro_rsrp_standard'][0]) >= int(self.config_mro['mr_lap'][0]):
                    temp_mro_main[2][17] = 1

                temp_id = 0

            if temp_value[7] == temp_value[11]:
                if temp_value[9] - int(self.config_mro['mro_rsrp_standard'][0]) >= int(self.config_mro[
                                                                                    'overlap_ncell_rsrp_1'][0]) and abs(
                                temp_value[0] - temp_value[9]) <= abs(int(self.config_mro['overlap_db_1'][0])):
                    temp_mro_main[2][9] = 1
                    temp_mro_main[2][10] = temp_value[2]
                    temp_mro_main[2][11] = temp_value[0]
                    temp_mro_main[2][12] = temp_value[9]

                if temp_value[9] - int(self.config_mro['mro_rsrp_standard'][0]) >= int(
                        self.config_mro['overlap_ncell_rsrp_2'][0]) and abs(
                                temp_value[0] - temp_value[9]) <= abs(int(self.config_mro['overlap_db_2'][0])):
                    temp_mro_main[2][13] = 1
                    temp_mro_main[2][14] = temp_value[2]
                    temp_mro_main[2][15] = temp_value[0]
                    temp_mro_main[2][16] = temp_value[9]

        # 先汇总，后才传送到queue
        try:
            self.temp_mro_data[report_time][temp_mro_main[0]][temp_mro_main[1]] += numpy.array(temp_mro_main[2])
        except:
            try:
                self.temp_mro_data[report_time][temp_mro_main[0]][temp_mro_main[1]] = numpy.array(temp_mro_main[2])
            except:
                try:
                    self.temp_mro_data[report_time][temp_mro_main[0]] = {}
                    self.temp_mro_data[report_time][temp_mro_main[0]][temp_mro_main[1]] = numpy.array(temp_mro_main[2])
                except:
                    self.temp_mro_data[report_time] = {}
                    self.temp_mro_data[report_time][temp_mro_main[0]] = {}
                    self.temp_mro_data[report_time][temp_mro_main[0]][temp_mro_main[1]] = numpy.array(temp_mro_main[2])

    def mro_ecid(self, object_mro, report_time):
        for value in object_mro.iter('v'):
            temp_value = list(map(int, value.text.rstrip().replace('NIL', '0').split(' ')))
            ecid1 = int(object_mro.attrib['id'])
            ecid_earfcn_pci_n_earfcn_n_pci = '_'.join(map(str, (ecid1,
                                                                temp_value[7],
                                                                temp_value[8],
                                                                temp_value[11],
                                                                temp_value[12]
                                                                )
                                                          )
                                                      )
            if temp_value[11] != 0:
                temp_mro_ecid = ['mro_ecid',
                                 ecid_earfcn_pci_n_earfcn_n_pci,
                                 [temp_value[0],
                                  temp_value[2],
                                  temp_value[9], 1,
                                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                  ]
                                 ]
                srsrp_nrsrp = temp_value[9] - temp_value[0]
                if srsrp_nrsrp < -10:
                    srsrp_nrsrp = -11
                if srsrp_nrsrp > 10:
                    srsrp_nrsrp = 11
                temp_mro_ecid[2][self.rsrp_dir[srsrp_nrsrp]] = 1
                # 先汇总，后才传送到queue
                try:
                    self.temp_mro_data[report_time][temp_mro_ecid[0]][temp_mro_ecid[1]] += numpy.array(temp_mro_ecid[2])
                except:
                    try:
                        self.temp_mro_data[report_time][temp_mro_ecid[0]][temp_mro_ecid[1]] = numpy.array(
                            temp_mro_ecid[2])
                    except:
                        try:
                            self.temp_mro_data[report_time][temp_mro_ecid[0]] = {}
                            self.temp_mro_data[report_time][temp_mro_ecid[0]][temp_mro_ecid[1]] = numpy.array(
                                temp_mro_ecid[2])
                        except:
                            self.temp_mro_data[report_time] = {}
                            self.temp_mro_data[report_time][temp_mro_ecid[0]] = {}
                            self.temp_mro_data[report_time][temp_mro_ecid[0]][temp_mro_ecid[1]] = numpy.array(
                                temp_mro_ecid[2])

    def mro_rsrp(self, mro_object, report_time):
        ecid = int(mro_object.attrib['id'])
        for value in mro_object.iter('v'):
            temp_value = list(map(int, value.text.rstrip().replace('NIL', '0').split(' ')))
            temp_mro_rsrp = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            temp_mro_rsrp[self.mro_rsrp_list[temp_value[0]]] = 1
            try:
                self.temp_mro_data[report_time]['mro_rsrp'][ecid] += numpy.array(temp_mro_rsrp)
            except:
                try:
                    self.temp_mro_data[report_time]['mro_rsrp'][ecid] = numpy.array(temp_mro_rsrp)
                except:
                    try:
                        self.temp_mro_data[report_time]['mro_rsrp'] = {ecid: numpy.array(temp_mro_rsrp)}
                    except:
                        self.temp_mro_data[report_time] = {}
                        self.temp_mro_data[report_time]['mro_rsrp'] = {ecid: numpy.array(temp_mro_rsrp)}
            break

    def get_report_time(self, tree):

        """获取报告时段"""

        report_time = '-'
        if 'hour' in self.config_main['gather_type']:
            for temp_file_header in tree.iter(tag='fileHeader'):
                report_time = temp_file_header.attrib['startTime'][
                              :temp_file_header.attrib['startTime'].find(':')]
                break
        return report_time

    def parse_process(self, mr_type):

        """多进程控制"""

        # 获取表头及生产数据库
        for n in self.parse_file_list[mr_type]:
            for o in self.parse_file_list[mr_type][n]:
                self.child_parse_process(mr_type, n, o, ishead=1)
                break
            break

        # 统计个数
        self.num_files = 0
        # 统计文件个数
        for temp_n in self.parse_file_list[mr_type]:
            self.num_files += len(self.parse_file_list[mr_type][temp_n])
        # parse_file_list 格式为 {'mrs':{'gz':[...],'xml':[...]},
        #                         'mro':{'gz':[...],'xml':[...]},
        #                        }
        # temp_i 为文件类型（gz or xml）
        # temp_j 为待处理文件
        process_manager = multiprocessing.Manager()
        process_queue = process_manager.Queue()
        # process_lock = process_manager.Lock()
        process_pool = multiprocessing.Pool(processes=int(self.config_main['process'][0]))
        process_listen = process_pool.apply_async(self.listen, args=(process_queue, mr_type,))
        jobs = []
        for n in self.parse_file_list[mr_type]:
            for o in self.parse_file_list[mr_type][n]:
                job = process_pool.apply_async(self.child_parse_process, args=(mr_type, n, o, process_queue))
                jobs.append(job)
        for job in jobs:
            job.get()

        process_queue.put('all_finish')
        process_pool.close()
        process_pool.join()
        # 汇总子进程数据
        if mr_type == 'mrs':
            self.mrs_data_data = process_listen.get()
        elif mr_type == 'mro':
            self.mro_data_data = process_listen.get()

    def filter(self, file_name, type_filter):
        filter_id = self.config_filter['filter_id']
        filter_type = self.config_filter['filter_type']
        filter_hour = self.config_filter['filter_hour']
        temp_filter_id = os.path.split(file_name)[-1][19:25]
        temp_filter_type = os.path.split(file_name)[-1][7:10].lower()
        temp_filter_hour = os.path.split(file_name)[-1][34:36]

        if (temp_filter_id in filter_id or filter_id == ['']) and (
                        temp_filter_type in filter_type or filter_type == ['']) and (
                        temp_filter_hour in filter_hour or filter_hour == ['']):
            if type_filter == 'xml' or type_filter == 'gz':
                if os.path.isfile(os.path.join(self.config_main['target_path'][0], os.path.split(file_name)[-1])):
                    pass
                else:
                    shutil.copyfile(file_name, os.path.join(self.config_main['target_path'][0], os.path.split(
                        file_name)[-1]))
            return 1
        else:
            return 0

    def child_parse_process(self, mr_type, file_type, file_name, queue='', ishead=0):

        """文件格式判断、解压、parse xml"""

        if file_type == 'xml':
            try:
                if self.config_filter['active_filter'] != ['1']:
                    tree = ET.parse(file_name)
                    self.parser(tree, mr_type, queue, ishead)
                else:
                    if self.filter(file_name, 'xml') == 1:
                        tree = ET.parse(file_name)
                        self.parser(tree, mr_type, queue, ishead)
            except:
                traceback.print_exc()

        elif file_type == 'gz':
            try:
                try:
                    tar_f = tarfile.open(file_name)
                    for temp_file in tar_f.getnames():
                        # print(temp_file)
                        temp_file_tar_f = tar_f.extractfile(temp_file)
                        temp_file_suffix = temp_file.split('.')[-1].lower()
                        if temp_file_suffix == 'gz':
                            if self.config_filter['active_filter'] != ['1']:
                                tree = ET.parse(gzip.open(temp_file_tar_f))
                                self.parser(tree, mr_type, queue, ishead)
                            else:
                                if self.filter(temp_file, 'tar_gz') == 1:
                                    tree = ET.parse(gzip.open(temp_file_tar_f))
                                    self.parser(tree, mr_type, queue, ishead)
                                    tar_f.extract(temp_file, self.config_main['target_path'][0])

                        elif temp_file_suffix == 'xml':
                            if self.config_filter['active_filter'] != ['1']:
                                tree = ET.parse(temp_file_tar_f)
                                self.parser(tree, mr_type, queue, ishead)
                            else:
                                if self.filter(temp_file, 'tar_gz') == 1:
                                    tree = ET.parse(temp_file_tar_f)
                                    self.parser(tree, mr_type, queue, ishead)
                                    tar_f.extract(temp_file, self.config_main['target_path'][0])

                    tar_f.close()
                except:
                    gzip_file = gzip.open(file_name)
                    if self.config_filter['active_filter'] != ['1']:
                        tree = ET.parse(gzip_file)
                        self.parser(tree, mr_type, queue, ishead)
                    else:
                        if self.filter(file_name, 'gz') == 1:
                            tree = ET.parse(gzip_file)
                            self.parser(tree, mr_type, queue, ishead)
            except:
                traceback.print_exc()

        # 数据送到queue
        if ishead == 0:
            type_list = {'mrs': self.temp_mrs_data,
                         'mro': self.temp_mro_data}
            self.queue_send(queue, type_list[mr_type])
            queue.put('prog')
            type_list[mr_type] = {}

    def parser(self, tree, mr_type, queue, ishead):
        if ishead == 0:
            if mr_type == 'mrs':
                report_time = self.get_report_time(tree)
                for temp_mr_name in tree.iter('measurement'):
                    temp_table_name_1 = temp_mr_name.attrib['mrName']
                    if temp_table_name_1 in self.mrs_parse_sheet:
                        for temp_id in temp_mr_name.iter('object'):
                            temp_mrs_ecid = temp_id.attrib['id']
                            for temp_value in temp_id.iter('v'):
                                temp_values = numpy.array(list(map(int, temp_value.text.rstrip().split(' '))))
                                try:
                                    self.temp_mrs_data[report_time][temp_table_name_1][temp_mrs_ecid] += temp_values
                                except:
                                    try:
                                        self.temp_mrs_data[report_time][temp_table_name_1][temp_mrs_ecid] = temp_values
                                    except:
                                        try:
                                            self.temp_mrs_data[report_time][temp_table_name_1] = {}
                                            self.temp_mrs_data[report_time][temp_table_name_1][
                                                temp_mrs_ecid] = temp_values
                                        except:
                                            self.temp_mrs_data[report_time] = {}
                                            self.temp_mrs_data[report_time][temp_table_name_1] = {}
                                            self.temp_mrs_data[report_time][temp_table_name_1][
                                                temp_mrs_ecid] = temp_values

            elif mr_type == 'mro':
                table_list = {'mro_main': self.mro_main,
                              'mro_ecid': self.mro_ecid,
                              'mro_rsrp': self.mro_rsrp}
                report_time = self.get_report_time(tree)
                for measurement in tree.iter('measurement'):
                    for smr in measurement.iter('smr'):
                        head_temp = smr.text.replace('.', '_').rstrip().split(' ')
                        if head_temp[0] == 'MR_LteScRSRP':
                            for object_mro in measurement.iter('object'):
                                # 分别生成需要的mro表
                                for table_temp in self.config_mro['mro_parse_sheet']:
                                    if table_temp != 'mro_ecid':
                                        table_list[table_temp](object_mro, report_time)
                                    else:
                                        table_list[table_temp](object_mro, '-')

        elif ishead == 1:
            # 获取需处理表名
            if mr_type == 'mrs':
                for mrname in tree.iter('measurement'):
                    if (self.config_mrs[
                            'mrs_parse_sheet'
                        ] == [''] or mrname.attrib['mrName'] in self.config_mrs['mrs_parse_sheet']) and mrname.attrib[
                        'mrName'
                    ] not in self.config_mrs['mrs_exception_sheet']:
                        self.mrs_parse_sheet.append(mrname.attrib['mrName'])

            # 获取mrs表头
            if mr_type == 'mrs':
                for mrname in tree.iter('measurement'):
                    temp_table_name = mrname.attrib['mrName']
                    if temp_table_name in self.mrs_parse_sheet:
                        for smr in mrname.iter('smr'):
                            head_temp = smr.text.replace('.', '_').rstrip().split(' ')
                            self.mrs_head[temp_table_name] = head_temp

    def queue_send(self, queue, data):
        for temp_report_time in data:
            for temp_table in data[temp_report_time]:
                for temp_ecid in data[temp_report_time][temp_table]:
                    queue.put(['data', [temp_report_time,
                                        temp_table,
                                        temp_ecid,
                                        data[temp_report_time][temp_table][temp_ecid]
                                        ]
                               ]
                              )

    def gather(self, mr_type):
        gather_data = {'mrs': {},
                       'mro': {}
                       }
        temp_gather_data = {}
        if mr_type == 'mrs':
            temp_gather_data = self.mrs_data_data
        elif mr_type == 'mro':
            temp_gather_data = self.mro_data_data
        for temp_table in temp_gather_data[mr_type]:
            if temp_table not in gather_data[mr_type]:
                gather_data[mr_type][temp_table] = {'-': {}}
            for temp_report_time in temp_gather_data[mr_type][temp_table]:
                for temp_ecid in temp_gather_data[mr_type][temp_table][temp_report_time]:
                    try:
                        gather_data[mr_type][temp_table]['-'][temp_ecid] += temp_gather_data[
                            mr_type][temp_table][temp_report_time][temp_ecid]
                    except:
                        gather_data[mr_type][temp_table]['-'][temp_ecid] = temp_gather_data[
                            mr_type][temp_table][temp_report_time][temp_ecid]
        if mr_type == 'mrs':
            self.mrs_data_data['mrs'] = gather_data['mrs']
        elif mr_type == 'mro':
            self.mro_data_data['mro'] = gather_data['mro']

    def listen(self, queue, mr_type):
        # 表结构
        # 表名:{time:
        #               {ecid:[值1，值2，......]
        #                   }
        #       }
        all_list = {'mrs': {},
                    'mro': {}
                    }
        num_ii = 0
        self.progress(self.all_num[mr_type], num_ii)
        while 1:
            value = queue.get()
            if value[0] == 'data':
                report_time = value[1][0]
                table_name = value[1][1]
                table_id = value[1][2]
                table_value = numpy.array(value[1][3])
                try:
                    all_list[mr_type][table_name][report_time][table_id] += table_value
                except:
                    try:
                        all_list[mr_type][table_name][report_time][table_id] = table_value
                    except:
                        try:
                            all_list[mr_type][table_name][report_time] = {}
                            all_list[mr_type][table_name][report_time][table_id] = table_value
                        except:
                            all_list[mr_type][table_name] = {}
                            all_list[mr_type][table_name][report_time] = {}
                            all_list[mr_type][table_name][report_time][table_id] = table_value
            elif value == 'prog':
                num_ii += 1
                self.progress(self.all_num[mr_type], num_ii)
            elif value == 'all_finish':
                return all_list

    def writer(self, mr_type, time_type):
        if self.config_main['timing'][0] == '1':
            temp_day_head = ''.join(('_', self.yesterday))
            temp_day = self.yesterday
        else:
            temp_day_head = ''
            temp_day = '-'
        if mr_type == 'mrs':
            for table_mrs in self.mrs_data_data['mrs']:
                with open(os.path.join(self.config_main['target_path'][0],
                                       '{0}{1}_{2}.csv'.format(table_mrs, temp_day_head, time_type)), 'w',
                          newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    if table_mrs == 'MR.RSRP':
                        writer.writerow(['DAY', 'TIME', 'ECID', 'ENB_ID', 'ENB_CELLID', 'MR覆盖率（RSRP>=-110)',
                                         'RSRP>=-110计数器', 'ALL计数器'] + self.mrs_head[table_mrs])
                        # print(self.mrs_data_data)
                        for temp_report_time in self.mrs_data_data['mrs'][table_mrs]:
                            for temp_ecid in self.mrs_data_data['mrs'][table_mrs][temp_report_time]:
                                enb_cellid = '_'.join((str(int(temp_ecid) // 256), str(int(temp_ecid) % 256)))
                                numerator = numpy.sum(
                                    self.mrs_data_data['mrs'][table_mrs][temp_report_time][temp_ecid][7:]
                                )
                                denominator = numpy.sum(
                                    self.mrs_data_data['mrs'][table_mrs][temp_report_time][temp_ecid]
                                )
                                if denominator == 0:
                                    range_mrs = '-'
                                else:
                                    range_mrs = round(numerator / denominator * 100, 2)
                                writer.writerow([temp_day, temp_report_time, temp_ecid, int(temp_ecid) // 256,
                                                 enb_cellid, range_mrs, numerator, denominator] + list(
                                    self.mrs_data_data['mrs'][table_mrs][temp_report_time][temp_ecid]))
                    else:
                        writer.writerow(['DAY', 'TIME', 'ECID', ] + self.mrs_head[table_mrs])
                        # print(self.mrs_data_data)
                        for temp_report_time in self.mrs_data_data['mrs'][table_mrs]:
                            for temp_ecid in self.mrs_data_data['mrs'][table_mrs][temp_report_time]:
                                writer.writerow([temp_day,
                                                 temp_report_time,
                                                 temp_ecid,
                                                 ] + list(
                                    self.mrs_data_data['mrs'][table_mrs][temp_report_time][temp_ecid]))
        elif mr_type == 'mro':
            # 读取基础数据
            self.mro_earfcn_pci_cellid_relate()
            for table in self.mro_data_data['mro']:
                with open(os.path.join(self.config_main['target_path'][0],
                                       '{0}{1}_{2}.csv'.format(table,
                                                               temp_day_head,
                                                               time_type)), 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    if table == 'mro_main':
                        writer.writerow(('DAY', 'TIME', 'ECID', 'ENBID', 'ENB_CLEEID', 'EARFCN', 'PCI', 'MR.LteScRSRP',
                                         'MR.LteScRSRQ', 'MR.LteScTadv', 'MR.LteScPHR', 'MR.LteScAOA', 'MR.LteScSinrUL',
                                         'MR.LteScPUSCHPRBNum', 'MR.LteScPDSCHPRBNum', 's_samplint',
                                         'overlap_-3_-113_samplint', 'overlap_-3_-113_ScTadv',
                                         'overlap_-3_-113_s_cell_rsrp', 'overlap_-3_-113_n_cell_rsrp',
                                         'overlap_-6_-113_samplint', 'overlap_-6_-113_ScTadv',
                                         'overlap_-6_-113_s_cell_rsrp', 'overlap_-6_-113_n_cell_rsrp',
                                         'RSRP>=-110采样点', 'MR覆盖率(RSRP>=-110)'))
                        for temp_report_time in self.mro_data_data['mro'][table]:
                            for table_id in self.mro_data_data['mro'][table][temp_report_time]:
                                temp_value = self.mro_data_data['mro'][table][temp_report_time][table_id]
                                if temp_value[8] != 0:
                                    temp_value_1 = temp_value[0:8] / temp_value[8]
                                else:
                                    temp_value_1 = temp_value[0:8]
                                if temp_value[9] != 0:
                                    temp_value_2 = temp_value[10:13] / temp_value[9]
                                else:
                                    temp_value_2 = temp_value[10:13]
                                if temp_value[13] != 0:
                                    temp_value_3 = temp_value[14:17] / temp_value[13]
                                else:
                                    temp_value_3 = temp_value[14:17]
                                temp_value = list(temp_value_1) + list(temp_value[8:10]) + list(temp_value_2) + list([
                                    temp_value[13], ]) + list(temp_value_3) + list([temp_value[17], ])
                                temp_value = list(map(int, temp_value))
                                temp_id = table_id.split('_')
                                temp_senbid = str(int(temp_id[0]) // 256)
                                writer.writerow([temp_day,
                                                 temp_report_time,
                                                 temp_id[0],
                                                 temp_senbid,
                                                 '_'.join((temp_senbid, str(int(temp_id[0]) % 256))),
                                                 temp_id[1],
                                                 temp_id[2]
                                                 ] + temp_value + [
                                    round(temp_value[17] / temp_value[8] * 100,
                                          2), ]
                                                )
                    elif table == 'mro_ecid':
                        writer.writerow(('DAY', 'TIME', 'S_ECID', 'ENBID', 'ENB_CELLID', 'S_EARFCN', 'S_PCI',
                                         'N_ENB_CELLID', 'N_EARFCN', 'N_PCI', 'Distance(m)', 'Scrsrp', 'ScTadv',
                                         'Ncrsrp', ' N_Samplint', '<-10db', '-10db', '-9db', '-8db', '-7db',
                                         '-6db', '-5db', '-4db', '-3db', '-2db', '-1db', '0db', '1db', '2db', '3db',
                                         '4db', '5db', ' 6db', '7db', '8db', '9db', '10db', '>10db'))
                        for temp_report_time in self.mro_data_data['mro'][table]:
                            for table_id in self.mro_data_data['mro'][table][temp_report_time]:
                                temp_value = self.mro_data_data['mro'][table][temp_report_time][table_id]
                                if temp_value[3] != 0:
                                    temp_value_1 = temp_value[0:3] / temp_value[3]
                                else:
                                    temp_value_1 = temp_value[0:3]
                                temp_value = list(temp_value_1) + list(temp_value[3:])
                                temp_value = list(map(int, temp_value))
                                temp_id = table_id.split('_')
                                temp_senbid = str(int(temp_id[0]) // 256)
                                temp_ncellid, temp_min_stance = self.min_distance_cell(
                                    temp_senbid, temp_id[3], temp_id[4]
                                )
                                writer.writerow([temp_day, temp_report_time, temp_id[0], temp_senbid,
                                                 '_'.join((str(int(temp_id[0]) // 256), str(int(temp_id[0]) % 256))),
                                                 temp_id[1], temp_id[2], temp_ncellid, temp_id[3], temp_id[4],
                                                 temp_min_stance] + temp_value)
                    elif table == 'mro_rsrp':
                        writer.writerow(('DAY', 'TIME', 'ECID', 'ENBID', 'ENB_CELLID', 'MR覆盖率（RSRP>=-110)',
                                         'RSRP>=-110计数器', 'ALL计数器',
                                         'MR_RSRP_00', 'MR_RSRP_01', 'MR_RSRP_02', 'MR_RSRP_03', 'MR_RSRP_04',
                                         'MR_RSRP_05', 'MR_RSRP_06', 'MR_RSRP_07', 'MR_RSRP_08', 'MR_RSRP_09',
                                         'MR_RSRP_10', 'MR_RSRP_11', 'MR_RSRP_12', 'MR_RSRP_13', 'MR_RSRP_14',
                                         'MR_RSRP_15', 'MR_RSRP_16', 'MR_RSRP_17', 'MR_RSRP_18', 'MR_RSRP_19',
                                         'MR_RSRP_20', 'MR_RSRP_21', 'MR_RSRP_22', 'MR_RSRP_23', 'MR_RSRP_24',
                                         'MR_RSRP_25', 'MR_RSRP_26', 'MR_RSRP_27', 'MR_RSRP_28', 'MR_RSRP_29',
                                         'MR_RSRP_30', 'MR_RSRP_31', 'MR_RSRP_32', 'MR_RSRP_33', 'MR_RSRP_34',
                                         'MR_RSRP_35', 'MR_RSRP_36', 'MR_RSRP_37', 'MR_RSRP_38', 'MR_RSRP_39',
                                         'MR_RSRP_40', 'MR_RSRP_41', 'MR_RSRP_42', 'MR_RSRP_43', 'MR_RSRP_44',
                                         'MR_RSRP_45', 'MR_RSRP_46', 'MR_RSRP_47'))
                        for temp_report_time in self.mro_data_data['mro'][table]:
                            for ecid_id in self.mro_data_data['mro'][table][temp_report_time]:
                                temp_value = self.mro_data_data['mro'][table][temp_report_time][ecid_id]
                                temp_value_1 = sum(temp_value[7:])
                                temp_value_2 = sum(temp_value)
                                if temp_value_2 != 0:
                                    temp_value_3 = round(temp_value_1/temp_value_2*100, 2)
                                else:
                                    temp_value_3 = '-'

                                temp_enbid = str(int(ecid_id) // 256)
                                temp_enb_cellid = '_'.join((str(int(ecid_id) // 256), str(int(ecid_id) % 256)))
                                writer.writerow([temp_day, temp_report_time, ecid_id, temp_enbid, temp_enb_cellid,
                                                 temp_value_3,
                                                 temp_value_1,
                                                 temp_value_2] + list(temp_value))

    def run_manager(self, mr_type):
        print('>>> 解码 ', mr_type.upper(), ' 数据...')
        self.get_config(mr_type)
        self.parse_process(mr_type)
        print('\n>>> {0} 计算及保存...'.format(mr_type))
        if 'hour' in self.config_main['gather_type']:
            self.writer(mr_type, 'hour')
            if 'ecid' in self.config_main['gather_type']:
                self.gather(mr_type)
                self.writer(mr_type, 'ecid')
        elif 'ecid' in self.config_main['gather_type']:
            self.writer(mr_type, 'ecid')
        print('>>> {0} 数据处理完毕！'.format(mr_type))
        print('-' * 26)
        print('完成！{0}解码结果保存在此文件夹: '.format(mr_type), self.config_main['target_path'][0])
        print('-' * 26)


if __name__ == '__main__':
    # print('main:', os.getpid())
    multiprocessing.freeze_support()
    star_time = time.time()
    # 初始化配置
    config_manager = Main()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    parse_type = config_manager.config_main['parse_type']
    # 统计获取到的文件数：
    exit_num = 0
    for temp_i in config_manager.parse_file_list:
        num = 0
        for temp_k in config_manager.parse_file_list[temp_i]:
            num += len(config_manager.parse_file_list[temp_i][temp_k])
        if num != 0:
            print('>>> 获取到 ', temp_i, ' 文件:', num)
            config_manager.all_num[temp_i] = num
            exit_num = 1
    if exit_num == 0:
        print('>>> {0} 没有获取到需处理数据，请检查！'.format(config_manager.config_main['source_path'][0]))
        sys.exit()

    for b in parse_type:
        if b in config_manager.parse_file_list:
            config_manager.run_manager(b.lower())
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
