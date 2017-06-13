import paramiko
import time
import os
import sys
import configparser

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
        self.config_main = {}
        self.config_ping = {}

    def get_ip(self):
        f = open(os.path.join(self.main_path,'ip.txt'))
        for i in f.readlines():
            self.ip.append(i.strip())

    def get_main_config(self):

        """获取 main 配置文件"""
        for a in self.cf.options('main'):
            self.config_main[a] = self.cf.get('main', a).split(',')

    def get_ping_config(self):

        for a in self.cf.options('ping'):
            self.config_ping[a] = self.cf.get('ping', a).split(',')

    def connect_ip(self,ip):
        try:
            # 连接IP
            print('>>> 正在连接：', ip, '...')
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, 22, "toor4nsn", "oZPS0POrRieRtu")
            print('>>> ', ip, '连接成功!')
            return 1, ssh
        except:
            print('>>> ', ip, ' 连接异常，请检查！')
            print('-' * 8)
            return 0, 0

    def cmd_ping(self, ssh, target_ip_list):
        result_list = {}
        for temp_ip in target_ip_list:
            cmd_text = ''.join(('ping ', temp_ip, ' -c 5'))
            print('发送指令...')
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
                cmd_result_format += cmd_result[-1][(cmd_result[-1].find('=')+1):].strip().split(' ')[0].split('/')
                result_list[temp_ip] = ['ok'] + cmd_result_format
            except:
                result_list[temp_ip] = ['ERROR', ]
                continue
        return result_list

    def cmd_process(self):
        result = {}
        for temp_ip in self.ip:
            connect_status, ssh = self.connect_ip(temp_ip)
            if connect_status:
                result[temp_ip] = self.cmd_ping(ssh, self.config_ping['target_ip'])
            else:
                continue
        return result

    def writer(self,list):
        f_write = open(os.path.join(self.main_path,'result.csv'),'w',encoding='utf-8-sig')
        f_write.write(','.join(('enbip','target_ip','status','packets transmitted','received','packet loss','min',
                                'avg', 'max','mdev','\n')))
        for i in list:
            for j in list[i]:
                f_write.write(i)
                f_write.write(',')
                f_write.write(j)
                f_write.write(',')
                f_write.write(','.join(list[i][j]))
                f_write.write('\n')

if __name__ == '__main__':
    manager = Main()
    manager.get_main_config()
    manager.get_ping_config()
    manager.writer(manager.cmd_process())



