import time
import os
import configparser
import sys
import datetime
import subprocess

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

2017-11-15 初版；


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
        self.config = {}
        cf = configparser.ConfigParser()
        cf.read(''.join((self.main_path, '\\', 'config.ini')), encoding='utf-8-SIG')
        for temp in cf.options('main'):
            self.config[temp] = cf.get('main', temp).split(',')

        # 设置并发门限值，控制在100以内
        if self.config['concurrent'][0] == ['']:
            self.config['concurrent'] = ['10']
        try:
            if int(self.config['concurrent'][0]) >= 100:
                self.config['concurrent'] = ['100']
        except:
            self.config['concurrent'] = ['10']

        # 自动建立程序运行当天文件夹
        if self.config['auto_creat_day_dir'] == ['1']:
            self.config['outdir'][0] = os.path.join(
                self.config['outdir'][0],
                datetime.date.today().strftime('%Y%m%d')
            )
        if not os.path.exists(self.config['outdir'][0]):
            os.makedirs(self.config['outdir'][0])

    def circuit(self):
        with open(os.path.join(self.main_path, 'addresses.txt')) as f_ip:
            ip_list = [temp_ip for temp_ip in f_ip]
        # 设置每次连接获取IP数量
        get_ip_num = 200
        if len(ip_list)//get_ip_num == 0:
            times = 1
        else:
            times = len(ip_list)//get_ip_num + 1
        for temp_times in range(times):
            f_temp_ip = open(os.path.join(self.config['outdir'][0], 'temp_addresses.txt'), 'w', encoding='utf-8')
            f_temp_ip.write(''.join(ip_list[temp_times*get_ip_num:temp_times*get_ip_num+get_ip_num]))
            f_temp_ip.close()

            # 调用工具，获取基站数据
            print('>>> 开始连接基站，获取log...')
            os.chdir(self.config['cli_path'][0])
            cmd_text = ' '.join((
                'collectfiles.bat',
                '-concurrent',
                self.config['concurrent'][0],
                self.config['cmd_parameter_comm'][0],
                ''.join((
                    self.config['outdir'][0],
                    '/temp_addresses.txt'
                )),
                '-outdir',
                self.config['outdir'][0],
                self.config['cmd_parameter_cust'][0],
            ))
            aa = subprocess.Popen(cmd_text, startupinfo=startupinfo)
            aa.wait()

if __name__ == '__main__':
    if os.name == 'nt':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    star_time = time.time()
    main = Main()
    main.circuit()
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
