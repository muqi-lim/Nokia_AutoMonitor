import time
import os
import sys
import datetime
import subprocess
import csv
from multiprocessing.dummy import Pool as ThreadPool

##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Author : lin_xu_teng
    E_mail : lxuteng@live.cn
--------------------------------
""")
print('\n')
exe_time = int(time.strftime('%Y%m%d', time.localtime(time.time())))
if exe_time > 20190101:
    print('\n')
    print('-' * 64)
    print('试用版本已过期，请联系作者！')
    print('-' * 64)
    print('\n')
    input()
    sys.exit()

print('''

update log:

2017-11-10 初版；
2017-11-14 修复激活测量时间不准确问题；


''')

print('\n')
print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n\n')
time.sleep(1)


###############################################################################

class Main:
    def __init__(self):
        print('>>> 程序初始化...')
        self.get_config()
        print('>>> 程序初始化完成!')

    def get_config(self):
        self.main_path = os.path.split(os.path.abspath(sys.argv[0]))[0]
        self.today = datetime.date.today().strftime('%Y%m%d')
        self.yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
        self.nowtime = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    def get_file_list(self):
        # 生成命令列表
        print('>>> 获取需激活测量基站列表...')
        self.cmd_list = []
        self.enabledpmmeasurement_list = {}
        # 生成BAT文件
        self.bat_file_list = {
            '拥塞': ['overcrowding', 'enabled_mtUEstate.xml'],
            'eSRVCC切换差小区': ['top_srvcc', 'enabled_actSrvccToGsm.xml'],
            'Volte低接通小区': ['top_volte_connect', 'enabled_mtEPSBearer.xml'],
            'Volte高丢包': ['top_volte_dldrop', 'enabled_mtQoS.xml'],
        }
        self.bat_path = ''.join((self.main_path, '/CommisionTool/temp/EnabeledPMMeasurement_', self.today, '.bat'))
        f = open(os.path.join(self.main_path,'HTML_TEMP/DisabeledPMMeasurementEnbList.csv'), 'r')
        f_csv = csv.reader(f)
        with open(self.bat_path, 'w') as f_em:
            for temp_log in f_csv:
                if [temp_log[0], temp_log[5]] == [self.yesterday, 'Successfully']:
                    temp_text = ''.join(('call commission.bat -ne ',
                                        temp_log[4],
                                         ' -pw Nemuadmin:nemuuser -parameterfile ',
                                         self.bat_file_list[temp_log[2]][1],
                                         ' |tee ./temp/',
                                         'Enble-',
                                         self.bat_file_list[temp_log[2]][0],
                                         '-',
                                         temp_log[3],
                                         '.log'))
                    if temp_log[2] not in self.enabledpmmeasurement_list:
                        self.enabledpmmeasurement_list[temp_log[2]] = []
                    if [temp_log[3], temp_log[4]] not in self.enabledpmmeasurement_list[temp_log[2]]:
                        self.enabledpmmeasurement_list[temp_log[2]].append([temp_log[3], temp_log[4]])

                    self.cmd_list.append(temp_text)
                    f_em.write(temp_text)
                    f_em.write('\n')
        f.close()
        print('>>> 获取完成！')

    @staticmethod
    def run_call(ii):
        return subprocess.call(ii, shell=True)

    def run_cmd(self):
        print('>>> 开启激活基站测量...')
        # 修改运行文件夹为批处理文件所在目录，并执行批处理程序；
        os.chdir(''.join((self.main_path, '/CommisionTool')))
        # subprocess.call(self.bat_path)
        pool = ThreadPool(processes=10)
        pool.map(self.run_call, self.cmd_list)
        pool.close()
        pool.join()
        print('>>> 测量激活完毕！')

    def write(self):
        print('>>> 生成激活测量log...')
        # 读取批处理程序运行结果，并生成csv记录表
        top_name_tran = {
            'overcrowding': '拥塞',
            'top_srvcc': 'eSRVCC切换差小区',
            'top_volte_connect': 'Volte低接通小区',
            'top_volte_dldrop': 'Volte高丢包',
        }
        f_csv = ''.join((self.main_path, '/HTML_TEMP/EnabeledPMMeasurementEnbList.csv'))
        if not os.path.exists(f_csv):
            f_csv_new = open(f_csv, 'w')
            f_csv_new.write('日期,激活测量时间,类型,enbid,ip,激活测量情况\n')
            f_csv_new.close()
        with open(f_csv, 'a') as f_dml:
            for temp_table in self.enabledpmmeasurement_list:
                for temp_enbid in self.enabledpmmeasurement_list[temp_table]:
                    f_dml.write(self.yesterday)
                    f_dml.write(',')
                    f_dml.write(self.nowtime)
                    f_dml.write(',')
                    f_dml.write(temp_table)
                    f_dml.write(',')
                    f_dml.write(str(temp_enbid[0]))
                    f_dml.write(',')
                    f_dml.write(str(temp_enbid[1]))
                    f_dml.write(',')
                    try:
                        with open(''.join((self.main_path,
                                           '/CommisionTool/temp/Enble-',
                                           self.bat_file_list[temp_table][0],
                                           '-',
                                           temp_enbid[0],
                                           '.log')), 'r') as f_log:
                            log_info = f_log.read()
                            if 'Successfully activated' in log_info:
                                f_dml.write('Successfully')
                            elif ' Connection from 0.0.0.0 exists' in log_info:
                                f_dml.write('Connection refused')
                            elif 'Cannot connect to' in log_info:
                                f_dml.write('Connection Failed')
                            elif 'Commissioning failed' in log_info:
                                f_dml.write('Commissioning failed')
                            elif 'Maximum number of connections has exceeded' in log_info:
                                f_dml.write('Maximum number of connections has exceeded')
                            elif 'Failed to get HW data' in log_info:
                                f_dml.write('Failed to get HW data')
                            else:
                                f_dml.write('Other Failed')
                        f_dml.write('\n')
                    except:
                        f_dml.write('\n')
        print('>>> 完成！请到 /HTML_TEMP/EnabeledPMMeasurementEnbList.csv 检查运行结果.')

    def circuit(self):
        self.get_file_list()
        self.run_cmd()
        self.write()

if __name__ == '__main__':
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    star_time = time.time()
    main = Main()
    main.circuit()
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
