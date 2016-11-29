import xml.etree.ElementTree as ET
import numpy
import configparser
import os
import sys
import datetime
import gzip
import tarfile
import time
# import copy

##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Version : 1.2.1
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
print('\n')

print(
    '''
update log:

2016-11-23 v1
2016-11-29 完成MRS解码


'''
)
print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n\n')
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
        # 获取 main 配置
        self.get_main_config()

        self.yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

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
        print('>>> 数据处理及保存，请等待...')
        num_i = len(config_manager.config_mrs['gather_type'])
        if num_i == 1:
            mrs_manager.data_writer(config_manager.config_mrs['gather_type'][0])
        elif num_i == 2:
            mrs_manager.data_writer('hour')
            mrs_manager.parser_plus()
            mrs_manager.data_writer('id')
        print('>>> MRS 数据处理完毕！')
        print('-' * 26)
        print('完成！解码结果保存在此文件夹: ', config_manager.config_main['target_path'][0])
        print('-' * 26)

    @staticmethod
    def star_mro_manager():
        """MRO解码"""
        print('>>> 开始处理 MRO 数据...')
        mro_manager = MroParser()
        config_manager.parse_process('mro', mro_manager)

    def parse_process(self, filetype, parsetype_class):
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
                if n == 'xml':
                    tree = ET.parse(o)
                    parsetype_class.parser(tree)
                elif n == 'gz':
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
                num_run += 1
        # 完成进度条
        config_manager.progress(num_n, num_run, 'done!\n')

    def get_files(self, path):

        """获取需处理文件"""

        if not os.path.exists(path):
            print('source_path 所设置的目录不存在，请检查！')
            sys.exit()
        for p in os.listdir(path):
            source_path = '\\'.join((path, p))
            if os.path.isdir(source_path):
                self.get_files(source_path)
            else:
                # 判断解码文件类型（MRS or MRO）
                temp_parse_type_num = 0
                for temp_x in self.config_main['parse_type']:
                    if temp_x in p.lower():
                        temp_parse_type_num = 1
                        p = [p, temp_x]
                        if temp_x not in self.parse_file_list:
                            self.parse_file_list[temp_x] = {}
                        break
                    else:
                        continue
                if temp_parse_type_num == 0:
                    continue
                # 判断文件类型（gz or xml）
                # 判断是否有文件后缀，如果没有，则跳过
                if '.' not in p[0]:
                    continue
                else:
                    file_type = p[0].split('.')[-1].lower()
                    # 如果不是 file_type 设置的文件格式，则跳过
                    if file_type not in self.config_main['file_type']:
                        continue
                    # 如果有且满足file_type设置的文件格式，则加入到parse_file_list中
                    if file_type not in self.parse_file_list[p[1]]:
                        self.parse_file_list[p[1]][file_type] = []
                self.parse_file_list[p[1]][file_type].append(source_path)

        if len(self.parse_file_list) == 0:
            print('未获取到源文件，请检查source_path是否设置正确或源文件是否存在！')
            sys.exit()

    # 进度条
    @staticmethod
    def progress(num_total, num_run, file_name=''):
        bar_len = 24
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
        for i in self.data_data:
            if config_main['timing'][0] == '1':
                if gather_type == 'hour':
                    f = open(''.join((config_main['target_path'][0], '\\', i, config_main['yesterday'], '_hour.csv')),
                             'w')
                elif gather_type == 'id':
                    f = open(''.join((config_main['target_path'][0], '\\', i, config_main['yesterday'], '.csv')), 'w')
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

            if config_main['timing'][0] == '1':
                f.write(config_main['yesterday'])
                f.write(',')

            for j in self.data_data[i]:
                for k in self.data_data[i][j]:
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

    def parser(self, tree):
        pass


if __name__ == '__main__':
    star_time = time.time()
    # 初始化配置
    config_manager = ConfigManager()

    parse_type = config_manager.config_main['parse_type']

    # 统计获取到的文件数：
    if len(parse_type) == 1:
        num = 0
        for temp_j in config_manager.parse_file_list[parse_type[0]]:
            num += len(config_manager.parse_file_list[parse_type[0]][temp_j])
        print('>>> 获取到 ', parse_type[0], ' 文件:', num)
    elif len(parse_type) == 2:
        num = 0
        for temp_i in config_manager.parse_file_list:
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

    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
