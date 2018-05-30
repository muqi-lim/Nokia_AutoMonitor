# -*- coding: utf-8 -*-

import os
import sys
import openpyxl
import traceback


class Main:
    # 初始化
    def __init__(self):
        # 获取程序所在目录
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]

        # 建立公共变量
        self.base_data = {
            'table_mapping': {
                'ui_inter': {},
                'inter_ui': {},
            },
            'monitoring_object_and_mapping': {
                'sheet_head': [],
                'sheet_data': {},
            },
            'data_table_head': {}

        }

    # 获取基础数据函数.
    def get_base_data(self):
        path_base_data = os.path.join(self.main_path, 'Base_Data.xlsx')
        f_base_data_wb = openpyxl.load_workbook(path_base_data, read_only=True)
        for temp_sheet_name in f_base_data_wb.sheetnames:
            temp_f_base_data_wb_sheet = f_base_data_wb[temp_sheet_name]
            # 读取表映射关系
            if temp_sheet_name == 'table_mapping':
                for temp_row in temp_f_base_data_wb_sheet.iter_rows():
                    self.base_data['table_mapping']['ui_inter'][temp_row[0].value] = temp_row[1].value
                    self.base_data['table_mapping']['inter_ui'][temp_row[1].value] = temp_row[0].value
            # 读取监控对象及规则
            elif temp_sheet_name == 'monitoring_object_and_mapping':
                temp_head_identify = 0
                for temp_row in temp_f_base_data_wb_sheet.iter_rows():
                    if temp_head_identify == 1:
                        # 按序获取行
                        temp_value = [i.value for i in temp_row]
                        # 从表头中获取关键字段索引号
                        active_monitor = temp_value[self.base_data['monitoring_object_and_mapping']['sheet_head'].index(
                            '监控部署状态')]
                        monitor_target = temp_value[self.base_data['monitoring_object_and_mapping']['sheet_head'].index(
                            '字段变量标识（M）')]
                        monitor_target_source = temp_value[self.base_data['monitoring_object_and_mapping'][
                            'sheet_head'].index('字段变量标识（源）')]
                        target_table_name = temp_value[self.base_data['monitoring_object_and_mapping'][
                            'sheet_head'].index('数据库源表名称（源）')]
                        monitor_sign_of_operation = temp_value[self.base_data['monitoring_object_and_mapping'][
                            'sheet_head'].index('运算符号')]
                        threshold_upper = temp_value[self.base_data['monitoring_object_and_mapping'][
                            'sheet_head'].index('上门限')]
                        threshold_lower = temp_value[self.base_data['monitoring_object_and_mapping'][
                            'sheet_head'].index('下门限')]
                        # 判断是否启用监控，如果是则继续获取关键字段
                        if active_monitor == 1:
                            if target_table_name not in self.base_data['monitoring_object_and_mapping']['sheet_data']:
                                self.base_data['monitoring_object_and_mapping']['sheet_data'][target_table_name] = {}
                            self.base_data['monitoring_object_and_mapping']['sheet_data'][target_table_name][
                                monitor_target] = [monitor_target_source,
                                                   monitor_sign_of_operation,
                                                   threshold_upper,
                                                   threshold_lower
                                                   ]
                    elif temp_head_identify == 0:
                        self.base_data['monitoring_object_and_mapping']['sheet_head'] = [i.value for i in temp_row]
                        temp_head_identify = 1
            else:
                if temp_sheet_name not in self.base_data['data_table_head']:
                    self.base_data['data_table_head'][temp_sheet_name] = {}
                # for i in temp_f_base_data_wb_sheet.iter_rows(min_row=1, max_row=1):
                #     print(i)
                for i in temp_f_base_data_wb_sheet.iter_rows(min_row=1, max_row=1):
                    temp_value_1 = [j.value for j in i]
                    print(temp_value_1)
                for i in temp_f_base_data_wb_sheet.iter_rows(min_row=2, max_row=2):
                    temp_value_2 = [j.value for j in i]
                    print(temp_value_2)

                # self.base_data['data_table_head'][temp_sheet_name]

        print(self.base_data)


if __name__ == '__main__':
    main = Main()
    # 获取基础数据
    main.get_base_data()
