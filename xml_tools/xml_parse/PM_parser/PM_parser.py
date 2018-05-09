import xml.etree.ElementTree as ET
import time
import os
import configparser
import sys
import datetime
import sqlite3
import csv


class Main:
    def __init__(self):
        print('>>> 程序初始化...')
        self.get_config()
        print('>>> 程序初始化完成!')
        if self.config['use_existing_db'] != ['1']:
            self.get_files()
        self.data = {}
        self.head_list = []

    # 进度条
    @staticmethod
    def progress(num_total, num_run, file_name=''):
        bar_len = 10
        hashes = '|' * int(num_run / num_total * bar_len)
        spaces = '_' * (bar_len - len(hashes))
        sys.stdout.write("\r%s %s %d%%  %s" % (str(num_run), hashes + spaces, int(num_run / num_total * 100),
                                               file_name))
        sys.stdout.flush()

    def get_config(self):
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.config = {}
        cf = configparser.ConfigParser()
        cf.read(''.join((self.main_path, '\\', 'config.ini')), encoding='utf-8-SIG')
        for temp in cf.options('main'):
            self.config[temp] = cf.get('main', temp).split(',')

        self.config['pm_list'] = [i.replace(' ', '_') for i in self.config['pm_list']]

    def get_files(self):
        print('>>> 获取数据...')
        self.file_list = []
        for root, dirs, files in os.walk(self.config['source_path'][0]):
            for file in files:
                if 'PM.BTS-' in file and '.xml' in file:
                    self.file_list.append(os.path.join(root, file))
        self.file_n = len(self.file_list)
        if self.file_n == 0:
            print('>>> 未获取到原始文件，请检查！')
            sys.exit()
        else:
            print('>>> 获取原始文件：', self.file_n)

    def parser(self, tree):
        root = tree.getroot()
        # 获取时间
        for temp_time in root:
            temp_pm_time = temp_time.attrib['startTime'].split('+')[0].split('.')[0]
            temp_pm_time = datetime.datetime.strptime(temp_pm_time, "%Y-%m-%dT%H:%M:%S")
            pm_time = temp_pm_time  # - datetime.timedelta(hours=8)
            pm_time = datetime.datetime.strftime(pm_time, "%Y%m%d%H%M")
            if pm_time not in self.data:
                self.data[pm_time] = {}
            break
        for temp_iter in root.iter(tag='PMMOResult'):
            for i in temp_iter.iter(tag='localMoid'):
                try:
                    temp_dn = '{0}_{1}'.format(i.text.split('-')[2][:6], i.text.split('-')[3].split('/')[0])
                except:
                    pass
            for j in temp_iter.iter(tag='NE-WBTS_1.0'):
                if j.attrib['measurementType'] in self.config['pm_list']:
                    if temp_dn not in self.data[pm_time]:
                        self.data[pm_time][temp_dn] = {}
                    for k in j:
                        if k.tag in self.config['counter_list']:
                            self.data[pm_time][temp_dn][k.tag] = k.text

    def create_db(self):
        if os.path.exists(os.path.join(self.config['target_path'][0], 'db.db')):
            os.remove(os.path.join(self.config['target_path'][0], 'db.db'))
        self.cx = sqlite3.connect(os.path.join(self.config['target_path'][0], 'db.db'))
        self.cu = self.cx.cursor()
        text = ''
        for temp_time in self.data:
            head_check = 0
            for temp_dn in self.data[temp_time]:
                temp_head = sorted(self.data[temp_time][temp_dn].keys())
                self.temp_head_num = len(temp_head)
                if self.temp_head_num == 3:
                    continue
                else:
                    head_check = 1
                    text += 'sdate'
                    text += ' '
                    text += 'integer'
                    text += ','
                    text += 'enb_cellid'
                    text += ' '
                    text += 'integer'
                    text += ','
                    for temp_head_item in temp_head:
                        text += temp_head_item
                        text += ' '
                        text += 'integer'
                        text += ','
                    break
            if head_check == 1:
                break
            else:
                continue
        cmd = "create table {0} ({1})".format('db', text[:-1])
        self.cu.execute(cmd)

    def data_input_db(self):
        temp_value_list = []
        text_n = '?,' * (self.temp_head_num + 2)
        cmd_cx = "insert into {0} values({1})".format('db', text_n[:-1])
        for temp_time in self.data:
            for temp_dn in self.data[temp_time]:
                temp_head = sorted(self.data[temp_time][temp_dn].keys())
                temp_value = [self.data[temp_time][temp_dn][temp_head_item] for temp_head_item in temp_head]
                if len(temp_value) == self.temp_head_num and len(temp_value) != 3:
                    temp_value_list.append([temp_time, temp_dn] + temp_value)
        self.cx.executemany(cmd_cx, temp_value_list)
        self.cx.commit()

    def executer(self):
        f = open(os.path.join(self.main_path, 'kpi_R&D.sql'), encoding='utf-8-sig')
        sql_scr = f.read()
        self.cu.execute(sql_scr)
        with open(os.path.join(self.config['target_path'][0], 'kpi_parser.csv'), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([i[0] for i in self.cu.description])
            writer.writerows(self.cu.fetchall())

    def circuit(self):
        if self.config['use_existing_db'] != ['1']:
            print('>>> 开始解码...')
            progress_n = 0
            for temp_file in self.file_list:
                progress_n += 1
                self.progress(self.file_n, progress_n, os.path.split(temp_file)[-1])
                tree = ET.parse(temp_file)
                self.parser(tree)

            self.progress(self.file_n, progress_n, 'parse finish!\n')

            print('>>> 解码完成，开始生成数据...')
            print('>>> 开始数据入库...')
            self.create_db()
            self.data_input_db()
            print('>>> 入库完成，开始查询计算...')
            self.executer()
        else:
            self.cx = sqlite3.connect(os.path.join(self.config['target_path'][0], 'db.db'))
            self.cu = self.cx.cursor()
            print('>>> 开始查询计算...')
            self.executer()


if __name__ == '__main__':
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    star_time = time.time()
    main = Main()
    main.circuit()
    print('-' * 32)
    print('>>> 完成！PM解码结果保存在此文件夹:：', main.config['target_path'][0])
    print('-' * 32)
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
