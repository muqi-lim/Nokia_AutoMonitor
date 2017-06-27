import sys
import time
import csv
import configparser
import os
import xlsxwriter


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
    2017-6-26 完成初版；
    2017-6-27 优化；
    

    ''')
    print('-' * 36)
    print('      >>>   starting   <<<')
    print('-' * 36)
    print('\n')
    time.sleep(1)


################################################################################

class Main:
    def __init__(self):
        copy_right()
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((self.main_path, '\\', 'config.ini')), encoding='utf-8-SIG')
        # 初始化配置列表
        self.config_main = {}
        self.get_main_config()
        self.file_list = {'path_1': {},
                          'path_2': {}
                          }
        self.get_files('path_1', self.config_main['path_1'][0])
        self.get_files('path_2', self.config_main['path_2'][0])
        # 最终结果
        self.result_list = {}
        self.result_full_list = {}

    def get_main_config(self):

        """获取 main 配置文件"""
        for a in self.cf.options('main'):
            self.config_main[a] = self.cf.get('main', a).split(',')

    def get_files(self, path_name, path):
        temp_file_list = os.listdir(path)
        for i in temp_file_list:
            if i.split('.')[-1] == 'csv':
                self.file_list[path_name][i.split('.')[0]] = os.path.join(path, i)

    def get_value(self, file_full_path, table_value):
        with open(file_full_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=',')
            fieldnames = next(reader)
            reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')
            for row in reader:
                id = row['object_id']
                row.pop('object_id')
                table_value[id] = row

    def get_value_plan(self, file_full_path,table_value):
        with open(file_full_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=',')
            fieldnames = next(reader)
            reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=',')
            for row in reader:
                id = row['object_id']
                para_name = row['参数名称']
                para_updata = row['修改值']
                if id not in table_value:
                    table_value[id] = {para_name: para_updata}
                else:
                    table_value[id][para_name] = para_updata

    def get_object_id(self, object):
        temp_head = []
        temp_value = []
        for i in object.split('/'):
            temp_head.append(i[:i.find('-')])
            temp_value.append(i[i.find('-')+1:])
        return [temp_head, temp_value]

    def contrast_file(self, table_name, table_1, table_2):
        for temp_object_id in table_1:
            if temp_object_id in table_2:
                for temp_para_name in table_1[temp_object_id]:
                    if temp_para_name in table_2[temp_object_id] and temp_para_name is not None:
                        # 统计main表
                        if temp_para_name not in self.result_list[table_name]:
                            self.result_list[table_name][temp_para_name] = [0, 0, 0]
                        self.result_list[table_name][temp_para_name][0] += 1
                        temp_para_value_1 = table_1[temp_object_id][temp_para_name]
                        temp_para_value_2 = table_2[temp_object_id][temp_para_name]
                        if temp_para_value_1 != temp_para_value_2:
                            self.result_list[table_name][temp_para_name][1] += 1
                            # 统计详情表
                            if table_name not in self.result_full_list:
                                self.result_full_list[table_name] = {}
                            if temp_object_id not in self.result_full_list[table_name]:
                                self.result_full_list[table_name][temp_object_id] = {}
                            if temp_para_name not in self.result_full_list[table_name][temp_object_id]:
                                self.result_full_list[table_name][temp_object_id][temp_para_name] = [temp_para_value_1,
                                                                                                     temp_para_value_2,
                                                                                                     '否']
                            # 与参数修改记录进行匹配
                            try:
                                if self.table_plan[temp_object_id][temp_para_name] == temp_para_value_2:
                                    self.result_list[table_name][temp_para_name][2] += 1
                                    self.result_full_list[table_name][temp_object_id][temp_para_name][2] = '是'
                            except:
                                pass
                        else:
                            pass
                    else:
                        pass
            else:
                pass

    def writer(self):
        with open(os.path.join(self.config_main['res_path'][0], 'result_main.csv'),'w', encoding='utf-8-sig') as \
                f_writer:
            f_writer.write(','.join(('表名', '参数名称', 'ID', '统计总数',
                                     '改动参数总数', '已申请修改总数', '\n')))
            for temp_table in self.result_list:
                for temp_para_name in self.result_list[temp_table]:
                    text = ','.join((temp_table,
                                     temp_para_name,
                                     '_'.join((temp_table, temp_para_name)),
                                     ','.join(list(map(str, self.result_list[temp_table][temp_para_name]))),
                                     '\n'
                                     ))
                    f_writer.write(text)

        with open(os.path.join(self.config_main['res_path'][0], 'result_full.csv'),'w', encoding='utf-8-sig') as \
                f_full_writer:
            f_full_writer.write(','.join(('object_ID',
                                          '表名',
                                          '参数名称',
                                          'ID',
                                          'value_path_1',
                                          'value_path_2',
                                          '是否工单修改',
                                          '\n')))
            for temp_table in self.result_full_list:
                for temp_object_id in self.result_full_list[temp_table]:
                    for temp_para_name in self.result_full_list[temp_table][temp_object_id]:
                        text = ','.join((temp_object_id,
                                         temp_table,
                                         temp_para_name,
                                         '_'.join((temp_table, temp_para_name)),
                                         ','.join(list(map(str,
                                                           self.result_full_list[temp_table][temp_object_id][temp_para_name]))),
                                         '\n'
                                         ))
                        f_full_writer.write(text)

    def writer_xlsx(self):
        workbook = xlsxwriter.Workbook(os.path.join(self.config_main['res_path'][0], 'result_main.xlsx'))
        worksheet = workbook.add_worksheet('main')
        worksheet.write_row(0, 0, ['表名', '参数名称', '统计总数','改动参数总数', '已申请修改总数'])
        row_num = 1
        for temp_table in self.result_list:
            for temp_para_name in self.result_list[temp_table]:
                text = [temp_table, temp_para_name] + self.result_list[temp_table][temp_para_name]
                worksheet.write_row(row_num, 0, text)
                row_num += 1

        temp_head = ['表名', '参数名称', 'value_path_1', 'value_path_2', '是否工单修改']
        for temp_table in self.result_full_list:
            temp_worksheet = workbook.add_worksheet(temp_table)
            temp_object_id_list = []
            for temp_object_id in self.result_full_list[temp_table]:
                temp_object_id_list = self.get_object_id(temp_object_id)[0]
                break
            temp_worksheet.write_row(0, 0, temp_object_id_list + temp_head)
            row_num = 1
            for temp_object_id in self.result_full_list[temp_table]:
                temp_object_id_list = self.get_object_id(temp_object_id)[1]
                for temp_para_name in self.result_full_list[temp_table][temp_object_id]:
                    text = temp_object_id_list + [temp_table,
                                                  temp_para_name]+self.result_full_list[temp_table][temp_object_id][temp_para_name]
                    temp_worksheet.write_row(row_num, 0, text)
                    row_num += 1
        workbook.close()

    def process(self):
        self.table_plan = {}
        temp_plan_path = os.path.join(self.main_path, '参数修改记录表.csv')
        self.get_value_plan(temp_plan_path, self.table_plan)
        for temp_table in self.file_list['path_1']:
            if temp_table in self.file_list['path_2']:
                self.table_1 = {}
                self.get_value(self.file_list['path_1'][temp_table], self.table_1)
                self.table_2 = {}
                self.get_value(self.file_list['path_2'][temp_table], self.table_2)
                if temp_table not in self.result_list:
                    self.result_list[temp_table] = {}
                self.contrast_file(temp_table, self.table_1, self.table_2)
            else:
                pass
        # self.writer()
        self.writer_xlsx()

if __name__ == '__main__':
    star_time = time.time()
    # 初始化配置
    config_manager = Main()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 程序正在运行，请耐心等待...')
    config_manager.process()
    print('>>> 完成！')
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
