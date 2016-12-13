import xml.etree.ElementTree as ET
import time
import os
import configparser
import sys
import gzip


class Main:
    def __init__(self):
        print('>>> 程序初始化...')
        self.get_config()
        print('>>> 程序初始化完成!')
        self.get_files()
        # 初始化公共变量
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

    def get_files(self):
        print('>>> 获取数据...')
        self.file_list = []
        for root, dirs, files in os.walk(self.config['source_path'][0]):
            for file in files:
                if 'ENB-PM-V2.6.0-EutranCellTdd' in file and '.xml.gz' in file:
                    self.file_list.append(os.path.join(root, file))
        self.file_n = len(self.file_list)
        if self.file_n == 0:
            print('>>> 未获取到原始文件，请检查！')
        else:
            print('>>> 获取原始文件：', self.file_n)

    def circuit(self):
        parse_type = ''
        if 'raw' in self.config['gather_type']:
            if parse_type == '':
                parse_type = 'raw'
        if 'hour' in self.config['gather_type']:
            if parse_type == '':
                parse_type = 'hour'
        if 'day' in self.config['gather_type']:
            if parse_type == '':
                parse_type = 'day'
        if 'none' in self.config['gather_type']:
            if parse_type == '':
                parse_type = 'none'

        progress_n = 0
        for file_item in self.file_list:
            progress_n += 1
            self.progress(self.file_n, progress_n, os.path.split(file_item)[-1])
            tree = ET.parse(gzip.open(file_item))
            self.parser(tree, parse_type)
        self.progress(self.file_n, progress_n, 'parse finish!\n')

        print('>>> 解码完成，开始生成数据...')
        self.write()

        type_chose = {1: 'raw', 2: 'hour', 3: 'day', 4: 'none'}
        type_transpose = dict(zip(type_chose.values(), type_chose.keys()))
        num_chose = type_transpose[self.data['gather_type']]+1
        if num_chose == 2 and type_chose[num_chose] in self.config['gather_type']:
            self.gather(13, 'hour')
            self.write()
            num_chose += 1
        if num_chose == 3 and type_chose[num_chose] in self.config['gather_type']:
            self.gather(10, 'day')
            self.write()
            num_chose += 1
        if num_chose == 4 and type_chose[num_chose] in self.config['gather_type']:
            self.gather(0, 'none')
            self.write()

    def parser(self, tree, type_1):
        self.data['gather_type'] = type_1
        for i in tree.getroot():
            # 获取report时间
            if i.tag == 'FileHeader':
                begin_time = '-'
                # end_time = '-'
                if type_1 == 'raw':
                    for j in i.iter(tag='BeginTime'):
                        begin_time = j.text
                    # for j in i.iter(tag='EndTime'):
                    #     end_time = j.text
                elif type_1 == 'hour':
                    for j in i.iter(tag='BeginTime'):
                        begin_time = j.text[0:13]
                    # for j in i.iter(tag='EndTime'):
                    #     end_time = j.text[0:13]
                elif type_1 == 'day':
                    for j in i.iter(tag='BeginTime'):
                        begin_time = j.text[0:10]
                    # for j in i.iter(tag='EndTime'):
                    #     end_time = j.text[0:10]
                # report_time = '&'.join((begin_time, end_time))
                report_time = begin_time
                if report_time not in self.data:
                    self.data[report_time] = {'head': {}, 'data': {}}

            # 获取data
            if i.tag == 'Measurements':
                # 提取表名
                for j in i.iter(tag='PmName'):
                    for k in j:
                        if k.attrib['i'] not in self.data[report_time]['head']:
                            self.data[report_time]['head'][k.attrib['i']] = k.text
                # 提取数据
                for j in i.iter(tag='PmData'):
                    for k in j:
                        enbname = k.attrib['UserLabel']
                        dn_temp = {i.split('=')[0]: i.split('=')[1] for i in k.attrib['Dn'].split(',') if
                                   len(i.split('=')) == 2}
                        enbid = dn_temp['EnbFunction'].split('-')[-1]
                        lncelid = dn_temp['EutranCellTdd'].split('-')[-1]
                        enb_cellid = '_'.join((enbid, lncelid))
                        dn = '&'.join((enbid, enb_cellid, enbname))
                        if dn not in self.data[report_time]['data']:
                            self.data[report_time]['data'][dn] = {}
                        for l in k.iter('V'):
                            head_temp = self.data[report_time]['head'][l.attrib['i']]
                            if '.' not in l.text:
                                temp_l = int(l.text)
                            else:
                                temp_l = float(l.text)
                            if head_temp not in self.data[report_time]['data'][dn]:
                                self.data[report_time]['data'][dn][head_temp] = temp_l
                            else:
                                self.data[report_time]['data'][dn][head_temp] += temp_l
                        for ll in k.iter('CV'):
                            head_temp_cv = self.data[report_time]['head'][ll.attrib['i']]
                            sn = []
                            sv = []
                            for m in ll.iter('SN'):
                                sn.append('-'.join((head_temp_cv, m.text)))
                            for n in ll.iter('SV'):
                                if '.' not in n.text:
                                    temp_n = int(n.text)
                                else:
                                    temp_n = float(n.text)
                                sv.append(temp_n)
                            if head_temp_cv in self.data[report_time]['data'][dn]:
                                self.data[report_time]['data'][dn][head_temp_cv] += sum(sv)
                            else:
                                self.data[report_time]['data'][dn][head_temp_cv] = sum(sv)
                            temp_dict = dict(zip(sn, sv))
                            for o in temp_dict.keys():
                                if o in self.data[report_time]['data'][dn]:
                                    self.data[report_time]['data'][dn][o] += temp_dict[o]
                                else:
                                    self.data[report_time]['data'][dn][o] = temp_dict[o]
                                if o not in self.head_list:
                                    self.head_list.append(o)
        self.head_list += self.data[report_time]['head'].values()
        self.data[report_time]['head'] = {}

        # for o in self.data:
        #     self.head_list += [p for p in self.data[o]['head'].values()]
    def gather(self, num, type_2):
        gather_data = {}
        for temp_day in self.data:
            if temp_day != 'gather_type':
                # gather_data_time = '-&-'
                gather_data_time = '-'
                if num != 0:
                    # star_time = temp_day.split('&')[0][:num]
                    # end_time = temp_day.split('&')[1][:num]
                    # gather_data_time = '&'.join((star_time, end_time))
                    gather_data_time = temp_day[:num]
                if gather_data_time not in gather_data:
                    gather_data[gather_data_time] = {'data': {}, 'head': {}}

                for temp_dn in self.data[temp_day]['data']:
                    if temp_dn not in gather_data[gather_data_time]['data']:
                        gather_data[gather_data_time]['data'][temp_dn] = {}
                    for temp_head in self.head_list:
                        if temp_head in self.data[temp_day]['data'][temp_dn]:
                            if temp_head in gather_data[gather_data_time]['data'][temp_dn]:
                                gather_data[gather_data_time]['data'][temp_dn][temp_head] += self.data[
                                    temp_day]['data'][temp_dn][temp_head]
                            else:
                                gather_data[gather_data_time]['data'][temp_dn][temp_head] = self.data[
                                    temp_day]['data'][temp_dn][temp_head]
        gather_data['gather_type'] = type_2
        self.data = gather_data

    def write(self):
        self.head_list = sorted(list(set(self.head_list)))
        filename = ''.join(('pm_', self.data['gather_type'], '.csv'))
        with open(os.path.join(self.config['target_path'][0], filename), 'w') as f:
            # f.write(','.join(('STAR_TIME', 'END_TIME', 'ENB_ID', 'ENB_CELL_ID', 'CELL_NAME')))
            f.write(','.join(('STAR_TIME', 'ENB_ID', 'ENB_CELL_ID', 'CELL_NAME')))
            f.write(',')
            f.write(','.join(self.head_list))
            f.write('\n')
            for temp_data in self.data:
                if temp_data != 'gather_type':
                    for temp_dn in self.data[temp_data]['data']:
                        # f.write(','.join((temp_data.replace('&', ','), temp_dn.replace('&', ','))))
                        f.write(','.join((temp_data, temp_dn.replace('&', ','))))
                        f.write(',')
                        for temp_head in self.head_list:
                            if temp_head in self.data[temp_data]['data'][temp_dn]:
                                f.write(str(self.data[temp_data]['data'][temp_dn][temp_head]))
                            else:
                                f.write('-')
                            f.write(',')
                        f.write('\n')

if __name__ == '__main__':
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    star_time = time.time()
    main = Main()
    print('>>> 开始解码...')
    main.circuit()
    print('-' * 25)
    print('>>> 完成！PM解码结果保存在此文件夹:：', main.config['target_path'][0])
    print('-' * 25)
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
