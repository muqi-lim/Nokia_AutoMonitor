# -*- coding: utf-8 -*-
import configparser
import datetime
import os
import stat
import subprocess
import sys
import time
import ftplib
import paramiko

##############################################################################
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

2016-7-21 功能完成；
2016-7-22 修复bug；
2016-09-09 完善功能，修复bug；
2016-12-4 增加对使用 FTP协议 服务器的支持；
2016-12-13 修复历遍服务器上文件夹时出错的bug；
2016-12-13 修复某些情况下调用外部exe失败问题；

''')
print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n')
time.sleep(1)
##############################################################################


class Getini:
    # 获取配置文件
    path = os.path.split(os.path.abspath(sys.argv[0]))[0]

    def __init__(self, inifile='config.ini', inipath=path):
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((inipath, '/', inifile)), encoding='utf-8-SIG')

    def autodowntools(self):
        # 服务器IP
        db_name_list = self.cf.options('ip')
        if len(db_name_list) == 0:
            print('未设置数据库，请检查！')
            exit()
        else:
            self.db_list = {}
            for db_i in db_name_list:
                db_list_i = self.cf.get('ip', db_i).split(',')
                if db_list_i[0] in self.db_list:
                    print(db_list_i[0], ' 名称重复，请检查!')
                    exit()
                else:
                    if len(db_list_i) == 3:
                        self.db_list[db_list_i[0]] = (db_list_i[1],
                                                      db_list_i[2])
                    else:
                        self.db_list[db_list_i[0]] = (self.cf.get(
                            'key', 'user'), self.cf.get('key', 'pwd'))

# 存放路径
        self.remote_path = self.cf.get('path', 'remote_path').split(',')
        # 存放本机路径
        self.local_path = self.cf.get('path', 'local_path')

        # filter
        self.filter_1 = self.cf.get('filter', 'filter_1').split(',')
        self.filter_2_1 = self.cf.get('filter', 'filter_2_1').split(',')
        self.filter_2_2 = self.cf.get('filter', 'filter_2_2').split(',')

        self.filter_3 = self.cf.get('filter', 'filter_3')

        # other
        self.classify = self.cf.get('other', 'classify')
        self.prefix_ip = self.cf.get('other', 'prefix_ip')
        self.prefix = self.cf.get('other', 'prefix')

        self.timing = self.cf.get('other', 'timing')
        self.timing_dyn = self.cf.get('other', 'timing_dyn')

        self.exe = self.cf.get('other', 'exe')

        # 定时功能
        if self.timing in ['', '0']:
            pass
        elif self.timing == '1':
            if int(time.strftime('%H', time.localtime(time.time(
            )))) == 0 and self.timing_dyn == '1':
                self.local_path = '/'.join(
                    (self.local_path,
                     (datetime.date.today() - datetime.timedelta(days=1)
                      ).strftime('%Y%m%d')))
                self.filter_2_1 = [(
                    datetime.date.today() - datetime.timedelta(days=1)
                ).strftime('%Y%m%d')]
            else:
                self.local_path = '/'.join((
                    self.local_path, datetime.date.today().strftime('%Y%m%d')))
                self.filter_2_1 = [datetime.date.today().strftime('%Y%m%d')]
        elif self.timing == '2':
            self.local_path = '/'.join(
                (self.local_path,
                 (datetime.date.today() - datetime.timedelta(days=1)
                  ).strftime('%Y%m%d')))
            self.filter_2_1 = [(
                datetime.date.today() - datetime.timedelta(days=1)
            ).strftime('%Y%m%d')]
        else:
            print('>>> timing 设置异常，请检查！')
            sys.exit()

        self.filter_2 = {}
        for i in self.filter_2_1:
            for j in self.filter_2_2:
                self.filter_2[''.join((i, j))] = [i, j]


class Db:
    def __init__(self, ip, user, pwd):
        self.ip = ip
        self.user = user
        self.pwd = pwd

# 连接FTP

    def ftp_connect(self):
        print('>>> loading:', self.ip, '...\n')
        try:
            try:
                T = paramiko.Transport(self.ip)
                T.connect(username=self.user, password=self.pwd)
                self.sftp = [paramiko.SFTPClient.from_transport(T), 'sftp']
                print('>>> SFTP connect successful!\n')
                self.status = 1
                return 1
            except:
                self.sftp = [ftplib.FTP(self.ip,self.user,self.pwd,timeout=5) ,'ftp']
                print('>>> FTP connect successful!\n')
                self.status = 1
                return 1
        except:
            print('>>> fail connect to:', self.ip, '\n')
            self.status = 0
            return 0

    def down_main(self, local_path, down_list, prefix_ip, prefix, classify):
        # 设置前缀
        if prefix_ip in ['', '0']:
            prefix_ip = ''
        elif prefix_ip == '1':
            prefix_ip = ''.join((self.ip, '_'))
        else:
            print('>>> prefix_ip 设置非法，请检查！！')
            sys.exit()
        if prefix != '':
            prefix = ''.join((prefix, '_'))
        progress = Progress(down_list[1])
        n = 1
        # print('>>> 下载开始...\n')
        for down_type_1 in down_list[0]:
            for down_type_2 in down_list[0][down_type_1]:
                for down_file in down_list[0][down_type_1][down_type_2]:
                    # 是否按类别存放在不同的文件夹中
                    if classify == '0':
                        self.new_local_path = '/'.join((local_path, self.ip))
                        ini.new_local_path = '/'.join((local_path, self.ip))

                    elif classify == '1':
                        self.new_local_path = '/'.join(
                            (local_path, down_type_1))
                        ini.new_local_path = '/'.join(
                            (local_path, down_type_1))

                    elif classify == '2':
                        self.new_local_path = '/'.join(
                            (local_path, ini.filter_2[down_type_2][0]))
                        ini.new_local_path = '/'.join(
                            (local_path, ini.filter_2[down_type_2][0]))

                    elif classify == '':
                        self.new_local_path = local_path
                        ini.new_local_path = local_path
                    else:
                        print('>>> classify 设置非法，请检查！！')
                        sys.exit()
                    new_local_file = ''.join(
                        (prefix, prefix_ip,
                         down_file[down_file.rfind('/') + 1:]))
                    new_local_fullname = '/'.join(
                        (self.new_local_path, new_local_file))
                    progress.progress(n, new_local_file)
                    if not os.path.exists(self.new_local_path):
                        os.makedirs(self.new_local_path)
                    if not os.path.exists(new_local_fullname):
                        if self.sftp[1] == 'sftp':
                            self.sftp[0].get(down_file, new_local_fullname)
                        elif self.sftp[1] == 'ftp':
                            with open(new_local_fullname, 'wb') as ff:
                                self.sftp[0].retrbinary("RETR %s" %down_file,ff.write)
                    n += 1


class Getfiles:
    def __init__(self, sftp=""):
        # self.path = path
        self.sftp = sftp

    def local_deep_getfiles(self, path):
        def walk(path):
            for i in os.listdir(path):
                full_i = '/'.join((path, i))
                if os.path.isdir(full_i):
                    walk(full_i)
                else:
                    self.filelist[0].append(full_i)
                    self.filelist[1] += 1

        if path == ['']:
            print('>>> 未设置 remote_path ，请检查！')
            sys.exit()
        else:
            self.filelist = [[], 0]
            for j in path:
                walk(j)

    def remote_deep_getfiles(self, path):
        def walk(path):
            if self.sftp[1] == 'sftp':
                for i in self.sftp[0].listdir_attr(path):
                    full_i = '/'.join((path, i.filename))
                    if stat.S_ISDIR(i.st_mode):
                        try:
                            walk(full_i)
                        except:
                            pass
                    else:
                        self.filelist[0].append(full_i)
                        self.filelist[1] += 1
            elif self.sftp[1] == 'ftp':
                for i in self.sftp[0].nlst(path):
                    if '.' in i:
                        self.filelist[0].append(i)
                        self.filelist[1] += 1
                    else:
                        try:
                            if 'lost+found' in i:
                                pass
                            else:
                                walk(i)
                        except:
                            pass



        if path == ['']:
            print('>>> 未设置 remote_path ，请检查！')
            sys.exit()
        else:
            self.filelist = [[], 0]
            for j in path:
                try:
                    walk(j)
                except:
                    continue

    def filter(self, filter1=[''], filter2=[], filetype=''):
        self.filterlist = [{}, 0]
        if self.filelist[1] == 0:
            pass
        else:
            for i in self.filelist[0]:
                for j in filter1:
                    for k in filter2:
                        i_filename = i[i.rfind('/') + 1:].lower()
                        if (j.lower() in i_filename) and (
                                k.lower() in i_filename) and (
                                    filetype.lower() in i_filename):
                            try:
                                self.filterlist[0][j][k].append(i)
                                self.filterlist[1] += 1
                            except:
                                try:
                                    self.filterlist[0][j][k] = []
                                except:
                                    self.filterlist[0][j] = {}
                                    self.filterlist[0][j][k] = []
                                self.filterlist[0][j][k].append(i)
                                self.filterlist[1] += 1
        return self.filterlist


# 进度条
class Progress:
    def __init__(self, len_):
        self.len_ = len_

    def progress(self, count_, filename=''):
        bar_len = 10
        hashes = '|' * int(count_ / self.len_ * bar_len)
        spaces = '_' * (bar_len - len(hashes))
        sys.stdout.write("\r%s %s %d  %s" %
                         (str(count_), hashes + spaces, self.len_, filename))
        sys.stdout.flush()


def patch_crypto_be_discovery():
    """
    Monkey patches cryptography's backend detection.
    Objective: support pyinstaller freezing.
    """

    from cryptography.hazmat import backends

    try:
        from cryptography.hazmat.backends.commoncrypto.backend import backend as be_cc
    except ImportError:
        be_cc = None

    try:
        from cryptography.hazmat.backends.openssl.backend import backend as be_ossl
    except ImportError:
        be_ossl = None

    backends._available_backends_list = [
        be for be in (be_cc, be_ossl) if be is not None
    ]


patch_crypto_be_discovery()

if __name__ == '__main__':
    # 获取配置文件
    ini = Getini()
    ini.autodowntools()
    ip_list = ini.db_list
    for ip in ip_list:
        ip_db = Db(ip, ip_list[ip][0], ip_list[ip][1])
        ip_db.ftp_connect()
        if ip_db.status == 1:
            getfile = Getfiles(ip_db.sftp)
            getfile.remote_deep_getfiles(ini.remote_path)
            getfile.filter(ini.filter_1, ini.filter_2, ini.filter_3)
            if getfile.filterlist[1] == 0:
                print('>>> 未获取到合适文件！')
                continue
            else:
                ip_db.down_main(ini.local_path, getfile.filterlist,
                                ini.prefix_ip, ini.prefix, ini.classify)
            print('\n')
            print('>>> Done')
            print('\n')
            print('-'*32)
            print('-'*32)
        else:
            continue

# 调用处理程序
    if ini.exe != '':
        try:
            print('=' * 32)
            print('>>> 开始运行处理程序...')
            print(ini.new_local_path)
            if ini.classify in ['0', '1', '2']:
                os.chdir(ini.new_local_path)
            else:
                os.chdir(ini.local_path)
            subprocess.call(ini.exe)
            print('=' * 32)
        except:
            print('>>> 程序调用失败！')
            print('=' * 32)

    print('\n\n')
    print('-' * 36)
    print('      >>>   All Done   <<<')
    print('-' * 36)
    print('\n\n')
