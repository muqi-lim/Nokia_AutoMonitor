import xml.etree.ElementTree as ET
import time
import os
import configparser
import sys
import smtplib
from email.mime.text import MIMEText
import datetime
import xlrd

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

2017-10-7 修复当基站数超过1400个时在线获取log数据失败的bug
2017-10-7 新增并发数控制，可以自定义并发在线获取基站数目
2017-10-19 新增ip基础信息，可以在处理结果中匹配对应IP的基站信息；


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
        # 初始化公共变量
        self.data = {}
        self.head_list = []
        # 闭锁小区限制个数
        self.block_num = 5

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

        # 设置并发门限值，控制在100以内
        if self.config['concurrent'][0] == ['']:
            self.config['concurrent'] = ['10']
        try:
            if int(self.config['concurrent'][0]) >= 100:
                self.config['concurrent'] = ['100']
        except:
            self.config['concurrent'] = ['10']

    def get_ip_info(self):
        self.ip_info_list = {}
        workbook = xlrd.open_workbook(''.join((self.main_path, '\\', 'Ip_Info.xlsx')))
        table = workbook.sheet_by_index(0)
        for i in range(table.nrows):
            self.ip_info_list[table.row_values(i)[0]] = list(map(str, table.row_values(i)[1:]))

    def get_files(self):
        print('>>> 获取本地数据...')
        self.file_list = []
        for root, dirs, files in os.walk(os.path.join(self.main_path, 'TEMP')):
            for file in files:
                if 'FrequencyHistory.xml' in file:
                    self.file_list.append(os.path.join(root, file))
        self.file_n = len(self.file_list)
        if self.file_n == 0:
            print('>>> 未获取到原始文件，请检查！')
            sys.exit()
        else:
            print('>>> 获取原始文件：', self.file_n)

    def time_diff(self, str_time):
        time_time = datetime.datetime.strptime(str_time, "%Y%m%d%H%M%S")
        time_time_diff = time_time + datetime.timedelta(hours=8)
        return datetime.datetime.strftime(time_time_diff, "%Y/%m/%d %H:%M:%S")

    def circuit(self):
        # 读取IP文件
        with open(os.path.join(self.main_path, 'addresses.txt')) as f_ip:
            ip_list = [temp_ip for temp_ip in f_ip]
        # 设置每次连接获取IP数量
        get_ip_num = 200
        if len(ip_list)//get_ip_num == 0:
            times = 1
        else:
            times = len(ip_list)//get_ip_num + 1
        for temp_times in range(times):
            f_temp_ip = open(os.path.join(self.main_path, 'TEMP', 'temp_addresses.txt'), 'w', encoding='utf-8')
            f_temp_ip.write(''.join(ip_list[temp_times*get_ip_num:temp_times*get_ip_num+get_ip_num]))
            f_temp_ip.close()

            # 调用工具，获取基站数据
            if self.config['online_get_file'] == ['1']:
                print('>>> 开始连接基站，获取log...')
                os.chdir(self.config['cli_path'][0])
                cmd_text = ''.join((
                    'collectfiles.bat  -pw Nemuadmin:nemuuser -attempts 1 -connectout 10 -timeout 10 -freqhistory '
                    '-ipfile ',
                    self.main_path,
                    '/TEMP/temp_addresses.txt -outdir ',
                    self.main_path,
                    '/TEMP',
                    ' -concurrent ',
                    self.config['concurrent'][0]
                ))
                os.system(cmd_text)
        # 获取本地log数据
        self.get_files()

        # 解码
        print('>>> 开始解码...')
        progress_n = 0
        for file_item in self.file_list:
            progress_n += 1
            self.progress(self.file_n, progress_n, os.path.split(file_item)[-1])
            try:
                tree = ET.parse(file_item)
                self.parser(tree, os.path.split(file_item)[-1])
            except:
                pass
        self.progress(self.file_n, progress_n, 'parse finish!\n')
        print('>>> 解码完成，开始获取IP基础信息...')
        # 获取IP信息
        try:
            self.get_ip_info()
            print('>>> IP基础信息获取完成，开始生成结果报表...')
        except:
            print('>>> 未获取到IP基础信息，开始生成结果报表...')
        self.now_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
        self.write()
        print('-' * 32)
        print('>>> 完成！解码结果保存在此文件夹:：', ''.join((main.main_path, '\TEMP')))
        print('-' * 32)

        # GPS结果检查
        if self.config['gps_error_check'] == ['1']:
            print('>>> 开始检查GPS是否异常...')

            self.gps_error_check()

            # 检查结果存在异常，进入下一步处理
            if len(self.dacword_error_list) != 0:
                print('>>> 存在GPS异常基站,请尽快处理！')

                # 闭锁基站
                if self.config['auto_locked'] == ['block']:
                    if len(self.dacword_error_list) > self.block_num:
                        print('>>> 异常基站个数超过限制个数，不启动紧急闭锁基站，请检查是否设置异常或手动闭锁基站...')
                    else:
                        print('>>> 开始紧急闭锁基站...')
                        self.enable_lock()
                        print('>>> 紧急闭锁基站完成！')

                # 生成 html 文件
                self.html()

                # 发送邮件
                if self.config['actemail'] == ['1']:
                    print('>>> 发送邮件...')
                    self.email()
            else:
                print('>>> 未存在异常！')

    def parser(self, tree, file_name):
        temp_data = {}
        data = {}
        self.data[file_name] = {}

        for i in tree.getroot():
            for j in i:
                for k in j:
                    temp_data[k.tag] = k.text
                data[temp_data['_observationTime']] = temp_data
        # 兼容所有
        # for i in tree.getiterator(tag='FrequencyHistoryDataReport'):
        #     for j in i:
        #         for k in j:
        #             temp_data[k.tag] = k.text
        #         data[temp_data['_observationTime']] = temp_data
            # 只保留TOP
            temp_head_list = sorted(map(int, data.keys()))
            temp_head_list_top_n = map(str, temp_head_list[-int(self.config['items'][0]):])
            for temp_i in temp_head_list_top_n:
                self.data[file_name][self.time_diff(temp_i)] = data[temp_i]

    def write(self):
        self.head_list_head = ['file_name',
                               'IP',
                               '_observationTime']
        self.head_list = ['_clockFrequencyDiff',
                          '_dacWord',
                          '_unitId',
                          '_rejectedSamplePc',
                          '_tuningMode',
                          '_referenceSource',
                          '_gpsSatelliteAmount']
        with open(os.path.join(self.main_path,
                               ''.join(('TEMP/FrequencyHistory-',
                                        self.now_time,
                                        '.csv'
                                        ))), 'w') as f:
            if len(self.ip_info_list) == 0:
                f.write(','.join(self.head_list_head + self.head_list))
            else:
                f.write(','.join(self.head_list_head + self.ip_info_list['IP'] + self.head_list))
            f.write('\n')
            for temp_file_name in self.data:
                for temp_time in self.data[temp_file_name]:
                    f.write(temp_file_name)
                    f.write(',')
                    temp_ip = temp_file_name.split('_')[0]
                    f.write(temp_ip)
                    f.write(',')
                    # temp_time_format = '_'.join((temp_time[0:8], temp_time[8:10], temp_time[10:12], temp_time[12:]))
                    # f.write(temp_time_format)
                    f.write(temp_time)
                    f.write(',')
                    if len(self.ip_info_list) == 0:
                        pass
                    else:
                        try:
                            f.write(','.join(self.ip_info_list[temp_ip]))
                            f.write(',')
                        except:
                            f.write('-,-,-,-,')
                    for temp_item in self.head_list:
                        if self.data[temp_file_name][temp_time][temp_item] is None:
                            f.write('-')
                        else:
                            f.write(self.data[temp_file_name][temp_time][temp_item])
                        f.write(',')
                    f.write('\n')

    def gps_error_check(self):
        self.dacword_error_list = {}
        for temp_file_name in self.data:
            for temp_time in self.data[temp_file_name]:
                temp_dacword = int(self.data[temp_file_name][temp_time]['_dacWord'])
                temp_ip = temp_file_name.split('_')[0]
                if temp_dacword >= int(self.config['dacword'][0]):
                    if temp_file_name.split('_')[0] not in self.dacword_error_list:
                        self.dacword_error_list[temp_ip] = {temp_time: temp_dacword}
                    else:
                        self.dacword_error_list[temp_ip][temp_time] = temp_dacword

    def enable_lock(self):
        os.chdir(self.config['cli_path'][0])
        for temp_ip in self.dacword_error_list:
            cmd_text = ''.join((
                'blockcell.bat  -pw Nemuadmin:nemuuser -attempts 1 -connectout 10 -timeout 10 -all '
                '-ne ',
                temp_ip,
                ' -outdir ',
                self.main_path,
                '/TEMP'
            ))
            os.system(cmd_text)

    def html(self):
        # html 头部
        self.MIMEtext = '''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>'''
        self.MIMEtext += self.config['subject'][0] + self.now_time
        self.MIMEtext += '''</title>
            </head>
            <body>'''
        # html开头文字
        if self.config['auto_locked'] == ['block']:
            if len(self.dacword_error_list) > self.block_num:
                self.MIMEtext += '''
                <h1><pre>HI，all! 以下基站检测到GPS异常，因异常基站个数超过限制个数，不启动紧急闭锁基站，请检查是否设置异常或手动闭锁基站...！</pre></h1>
                '''
            else:
                self.MIMEtext += '''
                <h1><pre>HI，all! 以下基站检测到GPS异常，已尝试将小区闭锁，请尽快检查处理！</pre></h1>
                '''
        else:
            self.MIMEtext += '''
            <h1><pre>HI，all! 以下基站检测到GPS异常，请尽快处理！</pre></h1>
            '''
        # 表格开头设置
        self.MIMEtext += '''
        <table border = '1' cellspacing="0">
        <thead align = 'center' style = "background:#F2F2F2"><tr>
        '''
        # 表头
        self.MIMEtext += '''
        <td><b>IP</b></td>
        <td><b>TIME</b></td>
        <td><b>dacWord</b></td>
        '''
        if len(self.ip_info_list) == 0:
            pass
        else:
            for temp_ip_value_head in self.ip_info_list['IP']:
                self.MIMEtext += '<td><b>'
                self.MIMEtext += temp_ip_value_head
                self.MIMEtext += '</b></td>'
        # 表内容（表头设置）
        self.MIMEtext += '''
        </thead><tbody align = 'center'>
        '''
        for temp_ip in self.dacword_error_list:
            for temp_time in self.dacword_error_list[temp_ip]:
                self.MIMEtext += '<tr>'
                self.MIMEtext += '<td>'
                self.MIMEtext += temp_ip
                self.MIMEtext += '</td>'
                self.MIMEtext += '<td>'
                self.MIMEtext += temp_time
                self.MIMEtext += '</td>'
                self.MIMEtext += '<td>'
                self.MIMEtext += str(self.dacword_error_list[temp_ip][temp_time])
                self.MIMEtext += '</td>'
                if len(self.ip_info_list) == 0:
                    pass
                else:
                    try:
                        for temp_ip_value in self.ip_info_list[temp_ip]:
                            self.MIMEtext += '<td>'
                            self.MIMEtext += temp_ip_value
                            self.MIMEtext += '</td>'
                    except:
                        self.MIMEtext += '<td>-</td><td>-</td><td>-</td><td>-</td>'
                self.MIMEtext += '</tr>'
        self.MIMEtext += '</tbody></table></body></html>'
        with open(os.path.join(self.main_path,
                               'TEMP',
                               ''.join((self.config['subject'][0], '-', self.now_time, '.html'))
                               ), 'w', encoding='utf-8') as f_html:
            f_html.write(self.MIMEtext)

    def email(self):
        print('>>> 登录email...')
        try:
            self.smtpObj = smtplib.SMTP()
            self.smtpObj.connect(self.config['mail_host'][0], 25)
            self.smtpObj.login(self.config['mail_user'][0], self.config['mail_pwd'][0])
            print('>>> 登录email成功。')
        except:
            print('>>> email登录失败，请检查！')
            sys.exit()
        # 邮件正文
        self.message = MIMEText(self.MIMEtext, 'html', 'utf-8')
        # 邮件标题
        if self.config['auto_locked'] == ['block'] and len(self.dacword_error_list) <= self.block_num:
            self.message['Subject'] = '【闭锁】' + self.config['subject'][0] + self.now_time
        else:
            self.message['Subject'] = '【未闭锁】' + self.config['subject'][0] + self.now_time
        # 邮件发送邮箱
        self.message['From'] = self.config['mail_user'][0]
        # 邮件收件人
        if len(self.config['receivers']) > 1:
            self.message['To'] = ';'.join(self.config['receivers'])
        else:
            self.message['To'] = self.config['receivers'][0]
        # 其他设置
        self.message["Accept-Language"] = "zh-CN"
        self.message["Accept-Charset"] = "ISO-8859-1,utf-8"
        # 发送邮件
        self.smtpObj.sendmail(self.config['mail_user'][0],
                              self.config['receivers'],
                              self.message.as_string())
        print('>>> email已发送！')


if __name__ == '__main__':
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
    star_time = time.time()
    main = Main()
    main.circuit()
    print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(time.time() - star_time)))
    print(time.strftime('%Y/%m/%d %H:%M:%S', time.localtime()))
