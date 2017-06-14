import paramiko
import time
import os
import sys
import configparser
import threadpool


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
    2017-6-13 完成初版，支持ping指令；
    2017-6-14 支持多线程，效率大幅提升；


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
        self.ip = []
        self.get_ip()
        self.get_main_config()
        self.get_login_config()
        self.get_ping_config()
        self.init_result()

    def init_result(self):
        self.result = {}
        if self.config_ping['active_ping'] == ['1']:
            self.result = {
                'ping': {}
            }

    def get_ip(self):
        f = open(os.path.join(self.main_path, 'ip.txt'))
        for i in f.readlines():
            self.ip.append(i.strip())

    def get_main_config(self):
        self.config_main = {}
        for a in self.cf.options('main'):
            self.config_main[a] = self.cf.get('main', a).split(',')

    def get_login_config(self):
        self.config_login = {}
        for a in self.cf.options('login'):
            self.config_login[a] = self.cf.get('login', a).split(',')

    def get_ping_config(self):
        self.config_ping = {}
        for a in self.cf.options('ping'):
            self.config_ping[a] = self.cf.get('ping', a).split(',')

    def connect_ip(self, ip):
        try:
            # 连接IP
            # print('>>> 正在连接：', ip, '...')
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                ip,
                int(self.config_login['port'][0]),
                self.config_login['username'][0],
                self.config_login['password'][0]
            )
            # print('>>> ', ip, '连接成功!')
            return 1, ssh
        except:
            # print('>>> ', ip, ' 连接异常，请检查！')
            # print('-' * 8)
            return 0, 0

    def cmd_ping(self, ssh, target_ip_list):
        result_list = {}
        for temp_ip in target_ip_list:
            cmd_text = ''.join(('ping ', temp_ip, ' -c ', self.config_ping['ping_num'][0]))
            # print('发送指令...')
            try:
                # 发送指令
                stdin_0, stdout_0, stderr_0 = ssh.exec_command(cmd_text, timeout=5)
                # 获取命令运行结果
                cmd_result = stdout_0.readlines()
                cmd_result_format = []
                i_temp = 0
                for i in [i.strip() for i in cmd_result[-2].split(',')]:
                    cmd_result_format.append(i[:i.find(' ')])
                    i_temp += 1
                    if i_temp == 3:
                        break
                cmd_result_format += cmd_result[-1][(cmd_result[-1].find('=') + 1):].strip().split(' ')[0].split('/')
                result_list[temp_ip] = ['ok'] + cmd_result_format
            except:
                result_list[temp_ip] = ['ERROR', ]
                continue
        return result_list

    def process(self):
        pool = threadpool.ThreadPool(int(self.config_main['thread_num'][0]))
        requests = threadpool.makeRequests(self.cmd_process, self.ip)
        [pool.putRequest(req) for req in requests]
        pool.wait()

    def cmd_process(self, ip):
        connect_status, ssh = self.connect_ip(ip)
        if connect_status:
            if self.config_ping['active_ping'] == ['1']:
                self.result['ping'][ip] = ['ok', self.cmd_ping(ssh, self.config_ping['target_ip'])]
        else:
            if self.config_ping['active_ping'] == ['1']:
                self.result['ping'][ip] = ['ERROR', ]

    def writer(self, list):
        if self.config_ping['active_ping'] == ['1']:
            f_write = open(os.path.join(self.main_path, 'ping_result.csv'), 'w', encoding='utf-8-sig')
            f_write.write(','.join(('enbip', 'enbip_status', 'target_ip', 'target_ip_status', 'packets transmitted',
                                    'received', 'packet loss', 'min', 'avg', 'max', 'mdev', '\n')))
            for temp_enbip in list['ping']:
                if list['ping'][temp_enbip][0] == 'ok':
                    for temp_aip in list['ping'][temp_enbip][1]:
                        f_write.write(temp_enbip)
                        f_write.write(',ok,')
                        f_write.write(temp_aip)
                        f_write.write(',')
                        f_write.write(','.join(list['ping'][temp_enbip][1][temp_aip]))
                        f_write.write('\n')
                else:
                    f_write.write(temp_enbip)
                    f_write.write(',ERROR\n')


if __name__ == '__main__':
    manager = Main()
    star_time = time.time()
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    manager.process()
    manager.writer(manager.result)
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
