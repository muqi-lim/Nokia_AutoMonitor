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
import copy
import csv
import traceback
import math


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

    2017-3-10 基于sqlite3重构；

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
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((self.main_path, '\\', 'config.ini')),encoding='utf-8-SIG')
        # 初始化配置列表
        self.config_main = {}
        self.config_mrs = {}
        self.config_mro = {}

        self.yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

        # 获取 main 配置
        self.get_main_config()

        # 生成结果一个空字典
        self.value_lists = {'mrs': {}, 'mro': []}

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
            self.config_main['process'][0] = int(multiprocessing.cpu_count()/2)
        elif self.config_main['process'][0] in ['max', '', '0']:
            self.config_main['process'][0] = int(multiprocessing.cpu_count())
        if self.config_main['process'][0] in ['1', 1]:
            self.config_main['process'][0] = '2'

        # 获取处理列表
        self.parse_file_list = {}
        self.get_files(self.config_main['source_path'][0])

    def get_config(self, mr_type):

        """获取配置"""

        if mr_type == 'mrs':
            for l in self.cf.options('MRS'):
                self.config_mrs[l] = self.cf.get('MRS', l).split(',')
            self.mrs_parse_sheet = []
            self.config_mrs['gather_type'] = [m.lower() for m in self.config_mrs['gather_type']]
            if self.config_mrs['gather_type'] == ['all']:
                self.config_mrs['gather_type'] = ['hour', 'id']
            if self.config_mrs['gather_type'] == ['id', 'hour']:
                self.config_mrs['gather_type'] = ['hour', 'id']
        elif mr_type == 'mro':
            for n in self.cf.options('MRO'):
                self.config_mro[n] = self.cf.get('MRO', n).split(',')

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

    def child_parse_process(self, mr_type, file_type, file_name, queue='', ishead=0):

        """文件格式判断、解压、parse xml"""

        # self.temp_value_lists = copy.deepcopy(self.value_lists)
        print('parser:',os.getpid())

        if file_type == 'xml':
            try:
                tree = ET.parse(file_name)
                self.parser(tree, mr_type, queue, ishead)
            except:
                traceback.print_exc()

        elif file_type == 'gz':
            try:
                try:
                    tar_f = tarfile.open(file_name)
                    for temp_file in tar_f.getnames():
                        temp_file_tar_f = tar_f.extractfile(temp_file)
                        temp_file_suffix = temp_file.split('.')[-1].lower()
                        if temp_file_suffix == 'gz':
                            tree = ET.parse(gzip.open(temp_file_tar_f))
                            self.parser(tree, mr_type, queue, ishead)
                        elif temp_file_suffix == 'xml':
                            tree = ET.parse(temp_file_tar_f)
                            self.parser(tree, mr_type, queue, ishead)
                    tar_f.close()
                except:
                    traceback.print_exc()
                    tree = ET.parse(gzip.open(file_name))
                    self.parser(tree, mr_type, queue, ishead)
            except:
                traceback.print_exc()
        # # 数据入库
        # if ishead == 0:
        #     try:
        #         queue.acquire()
        #         cx = sqlite3.connect(os.path.join(self.config_main['target_path'][0], 'temp/db.db'))
        #         if mr_type == 'mrs':
        #             for table_temp in self.temp_value_lists['mrs']:
        #                 if len(self.temp_value_lists['mrs'][table_temp]) != 0:
        #                     text_n = '?,' * len(self.temp_value_lists['mrs'][table_temp][0])
        #                     cmd_cx = "insert into {0} values({1})".format(table_temp, text_n[:-1])
        #                     cx.executemany(cmd_cx, self.temp_value_lists['mrs'][table_temp])
        #         elif mr_type == 'mro':
        #             if self.temp_value_lists['mro'] != 0:
        #                 text_n = '?,' * len(self.temp_value_lists['mro'][0])
        #                 cmd_cx = "insert into {0} values({1})".format('mro', text_n[:-1])
        #                 cx.executemany(cmd_cx, self.temp_value_lists['mro'])
        #         cx.commit()
        #         cx.close()
        #     finally:
        #         lock.release()
        #         self.temp_value_lists = copy.deepcopy(self.value_lists)

    def parser(self, tree, mr_type, queue, ishead):
        if ishead == 1:
            # 获取需处理表名
            if mr_type == 'mrs':
                for mrname in tree.iter('measurement'):
                    if (self.config_mrs['mrs_parse_sheet'] == [''] or mrname.attrib['mrName'] in self.config_mrs['mrs_parse_sheet']) and mrname.attrib['mrName'] not in self.config_mrs['mrs_exception_sheet']:
                        self.mrs_parse_sheet.append(mrname.attrib['mrName'])
            elif mr_type == 'mro':
                self.mro_parse_sheet = ['mro']

            # # 获取表头，并生成数据库
            # if not os.path.exists(os.path.join(self.config_main['target_path'][0], 'temp')):
            #     os.makedirs(os.path.join(self.config_main['target_path'][0], 'temp'))
            # cx = sqlite3.connect(os.path.join(self.config_main['target_path'][0], 'temp/db.db'))
            # cu = cx.cursor()

            if mr_type == 'mrs':
                pass
                # for mrname in tree.iter('measurement'):
                #     if mrname.attrib['mrName'] in self.mrs_parse_sheet:
                #         for smr in mrname.iter('smr'):
                #             head_temp = smr.text.replace('.', '_').rstrip().split(' ')
                #             text = 'reportTime text,startTime text,endTime text,enbid integer,ecid text,'
                #             for i in head_temp:
                #                 text += i
                #                 text += ' '
                #                 text += 'integer'
                #                 text += ','
                #             text = text[:-1]
                #             try:
                #                 self.value_lists['mrs'][mrname.attrib['mrName'].replace('.', '_')] = []
                #                 cmd = "create table {0} ({1})".format(mrname.attrib['mrName'].replace('.', '_'), text)
                #                 cu.execute(cmd)
                #             except:
                #                 pass
            elif mr_type == 'mro':
                pass
            #     for smr in tree.iter('smr'):
            #         head_temp = smr.text.replace('.', '_').rstrip().split(' ')
            #         if head_temp[0] == 'MR_LteScRSRP':
            #             head_temp = head_temp[:13] + head_temp[20:]
            #             # text = 'reportTime text,startTime text,endTime text,TimeStamp text,ecid integer,'
            #             text = 'TimeStamp text,ecid integer,'
            #             for i in head_temp:
            #                 text += i
            #                 text += ' '
            #                 text += 'integer'
            #                 text += ','
            #             # text += 'issconlyone integer,overlap3 integer,overlap6 integer'
            #             text += 'issconlyone integer'
            #             # text = text[:-1]
            #             try:
            #                 cmd = "create table {0} ({1})".format('mro', text)
            #                 cu.execute(cmd)
            #             except:
            #                 pass
            #             break
            #         else:
            #             pass
            # cx.close()
        elif ishead == 0:
            if mr_type == 'mrs':
                pass
                # for fileHeader in tree.iter('fileHeader'):
                #     start_time = fileHeader.attrib['startTime']
                #     end_time = fileHeader.attrib['endTime']
                #     report_time = fileHeader.attrib['reportTime']
                # for fileHeader in tree.iter('eNB'):
                #     enbid = fileHeader.attrib['id']
                #
                # for mrname in tree.iter('measurement'):
                #     if mrname.attrib['mrName'] in self.mrs_parse_sheet:
                #         try:
                #             for object in mrname.iter('object'):
                #                 for value in object.iter('v'):
                #                     value_list = [report_time, start_time, end_time, enbid, object.attrib[
                #                         'id']] + list(map(int, value.text.rstrip().split(' ')))
                #                     self.temp_value_lists['mrs'][mrname.attrib['mrName'].replace('.', '_')].append(
                #                         value_list)
                #         except:
                #             traceback.print_exc()
            elif mr_type == 'mro':
                # for fileHeader in tree.iter('fileHeader'):
                #     start_time = fileHeader.attrib['startTime']
                #     end_time = fileHeader.attrib['endTime']
                #     report_time = fileHeader.attrib['reportTime']
                temp_mro_data = {}
                # 计算ecid时对应表
                rsrp_dir = {
                    -11: 4, -10: 5, -9: 6, -8: 7, -7: 8, -6: 9, -5: 10, -4: 11, -3: 12,
                    -2: 13, -1: 14, 0: 15, 1: 16, 2: 17, 3: 18, 4: 19, 5: 20, 6: 21, 7: 22,
                    8: 23, 9: 24, 10: 25, 11: 26
                }
                for measurement in tree.iter('measurement'):
                    for smr in measurement.iter('smr'):
                        head_temp = smr.text.replace('.', '_').rstrip().split(' ')
                        if head_temp[0] == 'MR_LteScRSRP':
                            for object in measurement.iter('object'):
                                ecid = int(object.attrib['id'])
                                # 计算MRO_MAIN表
                                temp_id = 1
                                for value in object.iter('v'):
                                    temp_value = list(map(int,value.text.rstrip().replace('NIL','0').split(' ')))
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
                                                          ]
                                                         ]
                                        temp_id = 0
                                    if temp_value[7] == temp_value[11]:
                                        if temp_value[9]-140 >= int(self.config_mro['overlap_ncell_rsrp_1'][0]) and \
                                                        abs(temp_value[0]-temp_value[9]) <= abs(int(self.config_mro[
                                                                                                        'overlap_db_1'][
                                                                                                        0])):
                                            temp_mro_main[2][9] = 1
                                            temp_mro_main[2][10] = temp_value[2]
                                            temp_mro_main[2][11] = temp_value[0]
                                            temp_mro_main[2][12] = temp_value[9]

                                        if temp_value[9]-140 >= int(self.config_mro['overlap_ncell_rsrp_2'][0]) and \
                                                        abs(temp_value[0]-temp_value[9]) <= abs(int(self.config_mro[
                                                                                                        'overlap_db_2'][
                                                                                                        0])):
                                            temp_mro_main[2][13] = 1
                                            temp_mro_main[2][14] = temp_value[2]
                                            temp_mro_main[2][15] = temp_value[0]
                                            temp_mro_main[2][16] = temp_value[9]
                                    # 先汇总，后才传送到queue
                                    try:
                                        temp_mro_data[temp_mro_main[0]][temp_mro_main[1]] += numpy.array(
                                            temp_mro_main[2])
                                    except:
                                        try:
                                            temp_mro_data[temp_mro_main[0]][temp_mro_main[1]] = numpy.array(
                                                temp_mro_main[2])
                                        except:
                                            temp_mro_data[temp_mro_main[0]] = {}
                                            temp_mro_data[temp_mro_main[0]][temp_mro_main[1]] = numpy.array(
                                                temp_mro_main[2])
                                    # if queue.qsize() > 99999:
                                    #     time.sleep(5)
                                    # queue.put(['data', temp_mro_main])

                                # MRO_ECID表
                                for value in object.iter('v'):
                                    temp_value = list(map(int, value.text.rstrip().replace('NIL', '0').split(' ')))
                                    ecid1 = int(object.attrib['id'])
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
                                                          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
                                                          ]
                                                         ]
                                        srsrp_nrsrp = temp_value[9] - temp_value[0]
                                        if srsrp_nrsrp < -10:
                                            srsrp_nrsrp = -11
                                        if srsrp_nrsrp > 10:
                                            srsrp_nrsrp = 11
                                        temp_mro_ecid[2][rsrp_dir[srsrp_nrsrp]] = 1
                                        # 先汇总，后才传送到queue
                                        try:
                                            temp_mro_data[temp_mro_ecid[0]][temp_mro_ecid[1]] += numpy.array(
                                                temp_mro_ecid[2])
                                        except:
                                            try:
                                                temp_mro_data[temp_mro_ecid[0]][temp_mro_ecid[1]] = numpy.array(
                                                    temp_mro_ecid[2])
                                            except:
                                                temp_mro_data[temp_mro_ecid[0]] = {}
                                                temp_mro_data[temp_mro_ecid[0]][temp_mro_ecid[1]] = numpy.array(
                                                    temp_mro_ecid[2])
                                        # queue.put(['data', temp_mro_ecid])
                                # MRO_earfcn表
                                for value in object.iter('v'):
                                    temp_value = list(map(int, value.text.rstrip().replace('NIL', '0').split(' ')))
                                    if temp_value[11] != 0:
                                        pass
                for temp_table in temp_mro_data:
                    for temp_ecid in temp_mro_data[temp_table]:
                        queue.put(['data', [temp_table, temp_ecid, temp_mro_data[temp_table][temp_ecid]]])

    def listen(self, queue, mr_type):
        print('listen:', os.getpid())
        all_list = {'mrs': {},
                    'mro': {}
                    }
        while 1:
            value = queue.get()
            if value[0] == 'data':
                table_name = value[1][0]
                table_id = value[1][1]
                table_value = numpy.array(value[1][2])
                try:
                    all_list[mr_type][table_name][table_id] += table_value
                except:
                    try:
                        all_list[mr_type][table_name][table_id] = table_value
                    except:
                        all_list[mr_type][table_name] = {}
                        all_list[mr_type][table_name][table_id] = table_value

            elif value == 'all_finish':
                return all_list

    def writer(self, mr_type):
        if mr_type == 'mrs':
            pass
        elif mr_type == 'mro':
            # 读取基础数据
            self.mro_earfcn_pci_cellid_relate()

            for table in self.mro_data_data['mro']:
                with open(os.path.join(self.config_main['target_path'][0], '{0}.csv'.format(table)), 'w', newline='') \
                        as csvfile:
                    writer = csv.writer(csvfile)
                    if table == 'mro_main':
                        writer.writerow(('ECID','ENBID','ENB_CLEEID','EARFCN','PCI','MR.LteScRSRP','MR.LteScRSRQ',
                                         'MR.LteScTadv','MR.LteScPHR','MR.LteScAOA','MR.LteScSinrUL',
                                         'MR.LteScPUSCHPRBNum','MR.LteScPDSCHPRBNum','s_samplint','overlap_-3_-113_samplint','overlap_-3_-113_ScTadv','overlap_-3_-113_s_cell_rsrp','overlap_-3_-113_n_cell_rsrp','overlap_-6_-113_samplint','overlap_-6_-113_ScTadv','overlap_-6_-113_s_cell_rsrp','overlap_-6_-113_n_cell_rsrp'))
                        for table_id in self.mro_data_data['mro'][table]:
                            temp_value = self.mro_data_data['mro'][table][table_id]
                            if temp_value[8] != 0:
                                temp_value_1 = temp_value[0:8]/temp_value[8]
                            else:
                                temp_value_1 = temp_value[0:8]
                            if temp_value[9] != 0:
                                temp_value_2 = temp_value[10:13]/temp_value[9]
                            else:
                                temp_value_2 = temp_value[10:13]
                            if temp_value[13] != 0:
                                temp_value_3 = temp_value[14:]/temp_value[13]
                            else:
                                temp_value_3 = temp_value[14:]
                            temp_value = list(temp_value_1) + list(temp_value[8:10]) + list(temp_value_2) + list([
                                temp_value[13], ]) + list(temp_value_3)
                            temp_value = list(map(int, temp_value))
                            temp_id = table_id.split('_')
                            temp_senbid = str(int(temp_id[0]) // 256)
                            writer.writerow([temp_id[0],
                                             temp_senbid,
                                             '_'.join((temp_senbid, str(int(temp_id[0]) % 256))),
                                             temp_id[1], temp_id[2]] + temp_value)
                    elif table == 'mro_ecid':
                        writer.writerow(('S_ECID','ENBID','ENB_CELLID','S_EARFCN','S_PCI','N_ENB_CELLID','N_EARFCN',
                                         'N_PCI','Distance(m)',
                                         'Scrsrp',
                                         'ScTadv','Ncrsrp',' N_Samplint','<-10db','-10db','-9db','-8db','-7db',
                                         '-6db','-5db','-4db','-3db','-2db','-1db','0db','1db','2db','3db','4db','5db',' 6db','7db','8db','9db','10db','>10db'))
                        for table_id in self.mro_data_data['mro'][table]:
                            temp_value = self.mro_data_data['mro'][table][table_id]
                            if temp_value[3] != 0:
                                temp_value_1 = temp_value[0:3]/temp_value[3]
                            else:
                                temp_value_1 = temp_value[0:3]
                            temp_value = list(temp_value_1) + list(temp_value[3:])
                            temp_value = list(map(int, temp_value))
                            temp_id = table_id.split('_')
                            temp_senbid = str(int(temp_id[0])//256)
                            temp_ncellid, temp_min_stance = self.min_distance_cell(temp_senbid, temp_id[3], temp_id[4])
                            writer.writerow([temp_id[0],temp_senbid,
                                             '_'.join((str(int(temp_id[0])//256), str(int(temp_id[0]) % 256))),
                                             temp_id[1], temp_id[2], temp_ncellid, temp_id[3], temp_id[4],
                                             temp_min_stance] + temp_value)

    def run_manager(self, mr_type):
        print('>>> 解码 ', mr_type.upper(), ' 数据...')
        self.get_config(mr_type)
        self.parse_process(mr_type)
        print('>>> MRS数据处理及保存，请等待...')
        self.writer(mr_type)
        print('>>> MRS 数据处理完毕！')
        print('-' * 26)
        print('完成！MRS解码结果保存在此文件夹: ', self.config_main['target_path'][0])
        print('-' * 26)


if __name__ == '__main__':
    print('main:', os.getpid())
    copy_right()
    multiprocessing.freeze_support()
    star_time = time.time()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    # 初始化配置
    config_manager = Main()
    parse_type = config_manager.config_main['parse_type']
    # 统计获取到的文件数：
    for temp_i in config_manager.parse_file_list:
        num = 0
        for temp_k in config_manager.parse_file_list[temp_i]:
            num += len(config_manager.parse_file_list[temp_i][temp_k])
        print('>>> 获取到 ', temp_i, ' 文件:', num)
    for b in parse_type:
        if b in config_manager.parse_file_list:
            config_manager.run_manager(b.lower())
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
