# -*- coding: utf-8 -*-
# __author__ = 'linxuteng'

import os
import sys
import configparser
import time
import datetime
import cx_Oracle
import subprocess
# import prettytable
# import copy
# 邮件模块
import smtplib
from email.mime.text import MIMEText

##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Version : 1.2.0
    Author : lin_xu_teng
    E_mail : lxuteng@live.cn
--------------------------------
""")
print('\n')
exetime = int(time.strftime('%Y%m%d', time.localtime(time.time())))
if exetime > 20180101:
    print('\n')
    print('-' * 64)
    print('试用版本已过期，请联系作者！')
    print('-' * 64)
    print('\n')
    input()
    sys.exit()

print('''

update log:

2016-11-14 添加最大用户数检测通报；
2017-1-24 根据春节保障内容添加显示字段，方便监控时使用；
2017-2-8 增加部分TOP、最大用户数超过门限小区字段；
2017-2-9 新增自动关闭拥塞小区测量上报开关；
2017-2-13 监控粒度设置为 raw_monitor 时，当有符合相关条件的top小区，会在邮件主题分别使用【干扰】【拥塞】【休眠】【maxue】
            标识，已方便邮件查看;
2017-7-28 新增当同时激活多个网管指标监控时的区分标识
2017-9-24 兼容TL16A版本基站通报及监控；添加2017年下半年考核指标通报；
2017-10-16 新增零流量小区通报；


''')

print('\n')
print('-' * 36)
print('      >>>   starting   <<<')
print('-' * 36)
print('\n\n')
time.sleep(1)


###############################################################################


class Getini:
    # 获取配置文件
    path = os.path.split(os.path.abspath(sys.argv[0]))[0]

    def __init__(self, inifile='config.ini', inipath=path):
        self.cf = configparser.ConfigParser()
        self.cf.read(''.join((inipath, '/', inifile)), encoding='utf-8-SIG')

    def automonitor(self):
        self.main = {}
        # 表头判断
        self.subject_overcrowding = 0
        self.subject_sleep = 0
        self.subject_sleep_0 = 0
        self.subject_gps = 0
        self.subject_maxue = 0
        for h in self.cf.options('main'):
            self.main[h] = self.cf.get('main', h)

        self.actemail = self.main['actemail']
        if self.main['actemail'] == '1':
            self.email = {}
            for o in self.cf.options('email'):
                if o != 'receivers':
                    o_child = self.cf.get('email', o)
                else:
                    o_child = self.cf.get('email', o).split(',')
                if o_child in ('', ['']):
                    print('>>> email 设置异常，请检查！')
                    sys.exit()
                self.email[o] = o_child

        self.config = {}
        self.autodisabledpmmeasurementdata = ''
        for i in self.cf.options('config'):
            self.config[i] = self.cf.get('config', i)

        if self.config['timeint'] == '0' or '':
            self.config['timeint'] = 1
        else:
            try:
                self.config['timeint'] = int(self.config['timeint'])
            except:
                print('>>> timeint 必须为数字，请检查！')
                sys.exit()

        if self.config['timetype'] == 'day':
            self.starttime = (datetime.date.today() - datetime.timedelta(
                days=self.config['timeint'])).strftime('%Y%m%d')
            self.endtime = datetime.date.today().strftime('%Y%m%d')
            self.starttime_topn = (
                datetime.date.today() - datetime.timedelta(days=1)
            ).strftime('%Y%m%d')
            # self.endtime_topn = datetime.date.today().strftime('%Y%m%d')
            self.endtime_topn = self.starttime_topn
            self.mainsql = 'main_kpi_day'
            self.mainvosql = 'main_vokpi_day'
            self.top_volte_sql = 'top_volte_day'
            self.top_kpi_sql = 'top_kpi_day'
            self.sleepingcell_sql = 'sleepingcell'
            self.sleep_cell_16a_hour = 'sleep_cell_16a_hour'
            self.htmlname = (datetime.date.today() - datetime.timedelta(days=1)
                             ).strftime('%Y%m%d')
            self.maxue = 'maxue_day'
            if ini.actemail == '1':
                self.subject = self.email['subject'] + (
                    datetime.date.today() - datetime.timedelta(
                        days=1)).strftime('%Y%m%d')
        elif self.config['timetype'] == 'hour':
            self.starttime = (datetime.datetime.now() - datetime.timedelta(
                hours=self.config['timeint'])).strftime('%Y%m%d%H')
            self.endtime = datetime.datetime.now().strftime('%Y%m%d%H')
            self.starttime_topn = (
                datetime.datetime.now() - datetime.timedelta(hours=1)
            ).strftime('%Y%m%d%H')
            self.endtime_topn = datetime.datetime.now().strftime('%Y%m%d%H')
            self.mainsql = 'main_kpi_hour'
            self.mainvosql = 'main_vokpi_hour'
            self.top_volte_sql = 'top_volte_hour'
            self.top_kpi_sql = 'top_kpi_hour'
            self.sleepingcell_sql = 'sleepingcell'
            self.sleep_cell_16a_hour = 'sleep_cell_16a_hour'
            self.htmlname = datetime.datetime.now().strftime('%Y%m%d%H')
            self.maxue = 'maxue_hour'
            if self.actemail == '1':
                self.subject = self.email['subject'] + datetime.datetime.now(
                ).strftime('%Y%m%d%H')
        elif self.config['timetype'] == 'raw' or self.config[
            'timetype'] == 'raw_monitor':
            self.starttime = (
                datetime.datetime.now() - datetime.timedelta(minutes=(
                                                                         self.config[
                                                                             'timeint'] + 1) * 15)).strftime(
                '%Y%m%d%H%M')
            self.endtime = datetime.datetime.now().strftime('%Y%m%d%H%M')
            self.starttime_topn = (
                datetime.datetime.now() - datetime.timedelta(
                    minutes=2 * 15 - 1)).strftime('%Y%m%d%H%M')
            self.endtime_topn = datetime.datetime.now().strftime('%Y%m%d%H%M')
            self.mainsql = 'main_kpi_raw'
            self.mainvosql = 'main_vokpi_raw'
            self.top_volte_sql = 'top_volte_raw'
            self.top_kpi_sql = 'top_kpi_raw'
            self.htmlname = datetime.datetime.now().strftime('%Y%m%d%H%M')
            self.sleepingcell_sql = 'sleepingcell_raw'
            self.sleep_cell_16a_raw = 'sleep_cell_16a_raw'
            self.sleep_cell_16a_hour = 'sleep_cell_16a_hour'
            self.maxue = 'maxue_raw'
            if self.actemail == '1':
                self.subject = self.email['subject'] + datetime.datetime.now(
                ).strftime('%Y%m%d%H%M')
        else:
            print(">>> timetype 设置异常，请检查！")
        self.alarm_sql = 'alarm'

        self.db = {}
        for j in self.cf.options('db'):
            j_child = self.cf.get('db', j).split(',')
            if len(j_child) != 3:
                print('>>> [db]设置异常，请检查！')
                sys.exit()
            self.db[j] = j_child

        self.SQL_name = {}
        for k in self.cf.options('SQL_name'):
            k_child = self.cf.get('SQL_name', k).split(',')
            if len(k_child) != 3:
                print('>>> [SQL_name] 设置异常，请检查！')
                sys.exit()
            self.SQL_name[k] = k_child

        self.sqlpath = '/'.join((self.path, 'SQL'))
        self.displaytext = ''

        self.kpi = {}
        for n in self.cf.options('kpi'):
            n_child = self.cf.get('kpi', n).split(',')
            if len(n_child) != 2:
                print('>>> [kpi] 设置异常，请检查！')
                sys.exit()
            self.kpi[n.lower()] = n_child

    def cellinfo(self):
        f = open('/'.join((self.path, 'cellinfo.csv')), 'r', encoding='utf-8-sig')
        k = 0
        self.cellinfo_data = {}
        for i in f.readlines():
            if k == 0:
                self.cellinfo_head = i.split(',')
                k = 1
            else:
                self.cellinfo_data[i.split(',')[0]] = i.split(',')
        f.close()


class Db:
    def __init__(self, ip, user, pwd):
        self.ip = ip
        self.user = user
        self.pwd = pwd

    # 连接数据库

    def db_connect(self):
        print('>>> loading:', self.ip, '...\n')
        try:
            conn = cx_Oracle.connect(self.user, self.pwd, self.ip)
            self.cc = conn.cursor()
            self.connectstate = 1
            print('>>> 连接成功!')
        except:
            print('无法连接数据库：', self.ip, ',请检查网络或数据库设置!')
            self.connectstate = 0

    def getdata(self,
                sqlname,
                timetype='main',
                counter='default',
                threshold='0'):
        threshold = str(threshold)
        print(''.join(('>>> 开始获取数据 ', ini.SQL_name[sqlname][0], ' ...')))
        sqlfullname = ''.join(
            (ini.sqlpath, '/', ini.SQL_name[sqlname][0], '.sql'))
        try:
            f = open(sqlfullname)
        except:
            print('>>> ', sqlfullname, ' 不存在，请检查！')
        try:
            if timetype == 'main':
                sqltext = f.read().replace('&1', ini.starttime).replace(
                    '&2', ini.endtime)
            elif timetype == 'top':
                sqltext = f.read().replace('&1', ini.starttime_topn).replace(
                    '&2', ini.endtime_topn)
            # top小区counter自定义
            if counter != 'default':
                sqltext = sqltext.replace('&3', counter)
        except:
            print('>>>  ', sqlfullname, ' 时段设置异常，请检查！')
        # 设置top小区门限值
        try:
            sqltext = sqltext.replace('&4', threshold)
        except:
            pass

        try:
            self.dateget = self.cc.execute(sqltext)
        except:
            print('>>> 查询出错，请检查SQL脚本！！', sqlfullname)

    def displaydata(self, datatype='main'):
        def main():
            self.kpi_dict = ini.kpi

        def top_srvcc():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'esrvcc成功率'.lower(): ('', ''),
                             'ESRVCC请求次数'.lower(): ('', ''),
                             'ESRVCC成功次数'.lower(): ('', ''),
                             'ESRVCC切换失败次数'.lower(): ('!=', '0'),
                             'QCI1话务量Erl'.lower(): ('', ''),
                             'QCI1最大用户数'.lower(): ('', ''),
                             'esrvcc切换失败次数ZB'.lower(): ('!=', '0'),
                             'esrvcc请求次数ZB'.lower(): ('', ''),
                             'esrvcc成功率ZB'.lower(): ('', ''),
                             }

        def top_qci1connect():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'qci1_erab建立成功率'.lower(): ('', ''),
                             'qci1_erab建立请求次数'.lower(): ('', ''),
                             'qci1_erab建立失败次数'.lower(): ('!=', '0'),
                             'QCI1话务量Erl'.lower(): ('', ''),
                             'QCI1最大用户数'.lower(): ('', ''),
                             }

        def top_qci2connect():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'qci2_erab建立成功率'.lower(): ('', ''),
                             'qci2_erab建立请求次数'.lower(): ('', ''),
                             'qci2_erab建立失败次数'.lower(): ('!=', '0'),
                             'QCI2话务量Erl'.lower(): ('', ''),
                             'QCI2最大用户数'.lower(): ('', ''),
                             }

        def top_qci1drop():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'QCI1掉线率小区级'.lower(): ('', ''),
                             'QCI1掉线分母小区级'.lower(): ('', ''),
                             'QCI1掉线次数'.lower(): ('!=', '0'),
                             'QCI1话务量Erl'.lower(): ('', ''),
                             'QCI2话务量Erl'.lower(): ('', ''),
                             'QCI1最大用户数'.lower(): ('', ''),
                             'QCI2最大用户数'.lower(): ('', ''),
                             }

        def top_rrcconnect():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             'RRC连接建立成功率'.lower(): ('', ''),
                             'RRC连接建立请求次数'.lower(): ('', ''),
                             'RRC连接建立失败次数'.lower(): ('!=', '0'),
                             '拥塞次数'.lower(): ('>', '0'),
                             '控制面过负荷拥塞'.lower(): ('>', '0'),
                             '用户面过负荷拥塞'.lower(): ('>', '0'),
                             'PUCCH资源不足拥塞'.lower(): ('>', '0'),
                             '最大RRC受限拥塞'.lower(): ('>', '0'),
                             'MME过负荷拥塞'.lower(): ('>', '0'),
                             'RRC最大连接数'.lower(): ('', ''),
                             '最大激活用户数'.lower(): ('', ''),
                             '有效RRC连接最大数'.lower(): ('', ''),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def top_erabconnect():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             'ERAB建立成功率'.lower(): ('', ''),
                             'ERAB建立成功数'.lower(): ('', ''),
                             'ERAB建立请求数'.lower(): ('', ''),
                             'ERAB建立失败数'.lower(): ('!=', '0'),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def top_handover():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             '切换成功率QQ'.lower(): ('', ''),
                             '切换成功次数'.lower(): ('', ''),
                             '切换请求次数ZB'.lower(): ('', ''),
                             '切换请求次数QQ'.lower(): ('', ''),
                             '切换失败次数QQ'.lower(): ('!=', '0'),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def top_radiodrop():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             '无线掉线率'.lower(): ('', ''),
                             '切换失败次数QQ'.lower(): ('!=', '0'),
                             '切换请求次数QQ'.lower(): ('', ''),
                             '切换请求次数ZB'.lower(): ('', ''),
                             '无线掉线率分母'.lower(): ('', ''),
                             '无线掉线率分子'.lower(): ('!=', '0'),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def top_erabdrop():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             'ERAB掉线率'.lower(): ('', ''),
                             'ERAB掉线率分母'.lower(): ('', ''),
                             'ERAB掉线次数'.lower(): ('!=', '0'),
                             '切换失败次数QQ'.lower(): ('!=', '0'),
                             '切换请求次数QQ'.lower(): ('', ''),
                             '切换请求次数ZB'.lower(): ('', ''),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def sleepingcell():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             '休眠时段数'.lower(): ('', ''),
                             'DRX激活子帧数'.lower(): ('', ''),
                             'DRX睡眠子帧数'.lower(): ('', '')}

        def sleep_cell_16a():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'BTS_VERSION'.lower(): ('', ''),
                             '休眠时段数'.lower(): ('', ''),
                             '带宽'.lower(): ('', ''),
                             '管理员闭锁状态'.lower(): ('', ''),
                             'EARFCN'.lower(): ('', ''),
                             'TYPE'.lower(): ('', ''),
                             '可用率'.lower(): ('', ''),
                             'RRC连接数'.lower(): ('', ''),
                             '最大RRC连接数'.lower(): ('', ''),
                             'RRC连接建立成功次数'.lower(): ('', ''),
                             'SRB1建立尝试次数'.lower(): ('', ''),
                             'SRB1建立成功次数'.lower(): ('', ''),
                             'SRB1建立失败次数'.lower(): ('', ''),
                             'DRX激活子帧数'.lower(): ('', ''),
                             'DRX睡眠子帧数'.lower(): ('', ''),
                             '昨天总流量MB'.lower(): ('', ''),
                             }

        def overcrowding():
            self.kpi_dict = {'enb_cell'.lower(): ('', 'cellname'),
                             'bts_version'.lower(): ('', ''),
                             'actDrx'.lower(): ('', ''),
                             'RRC连接建立成功率'.lower(): ('', ''),
                             'RRC连接建立请求次数'.lower(): ('', ''),
                             'RRC连接建立失败次数'.lower(): ('!=', '0'),
                             '拥塞次数'.lower(): ('>', '0'),
                             '控制面过负荷拥塞'.lower(): ('>', '0'),
                             '用户面过负荷拥塞'.lower(): ('>', '0'),
                             'PUCCH资源不足拥塞'.lower(): ('>', '0'),
                             '最大RRC受限拥塞'.lower(): ('>', '0'),
                             'MME过负荷拥塞'.lower(): ('>', '0'),
                             'RRC最大连接数'.lower(): ('', ''),
                             '最大激活用户数'.lower(): ('', ''),
                             '有效RRC连接最大数'.lower(): ('', ''),
                             'PUSCH_RIP'.lower(): ('>=', '-110')}

        def alarm():
            self.kpi_dict = {'lnbtsid'.lower(): ('', ''),
                             'IP'.lower(): ('', ''),
                             'NAME'.lower(): ('', ''),
                             'ALARM_TIME'.lower(): ('', ''),
                             'CANCEL_TIME'.lower(): ('', ''),
                             'SUPPLEMENTARY_INFO'.lower(): ('', '')}

        def maxue():
            self.kpi_dict = {
                'ENB_CELL'.lower(): ('', 'cellname'),
                'VERSION'.lower(): ('', ''),
                '带宽'.lower(): ('', ''),
                'actDrx'.lower(): ('', ''),
                '最大激活用户数'.lower(): ('', ''),
                '门限值'.lower(): ('', ''),
                '配置最大激活用户数'.lower(): ('', ''),
                'cqi资源'.lower(): ('', ''),
                'sr资源'.lower(): ('', ''),
                'maxNumActUE'.lower(): ('', ''),
                'maxNumRrc'.lower(): ('', ''),
                'maxNumRrcEmergency'.lower(): ('', '')
            }

        datatypelist = {'main': main,
                        'top_srvcc': top_srvcc,
                        'top_qci1connect': top_qci1connect,
                        'top_qci2connect': top_qci2connect,
                        'top_qci1drop': top_qci1drop,
                        'top_rrcconnect': top_rrcconnect,
                        'top_erabconnect': top_erabconnect,
                        'top_handover': top_handover,
                        'top_radiodrop': top_radiodrop,
                        'top_erabdrop': top_erabdrop,
                        'sleepingcell': sleepingcell,
                        'sleep_cell_16a': sleep_cell_16a,
                        'overcrowding': overcrowding,
                        'alarm': alarm,
                        'maxue': maxue}

        datatypelist[datatype]()
        self.headlist = [
            (child[0].lower(), self.dateget.description.index(child),
             self.kpi_dict[child[0].lower()])
            for child in self.dateget.description
            if child[0].lower() in self.kpi_dict
        ]
        self.headdata = [i[0] for i in self.headlist]
        self.headindex = [i[1] for i in self.headlist]
        self.kpirange = {i[1]: i[2] for i in self.headlist}
        self.head_data_index = dict(zip(self.headdata, self.headindex))
        # self.table = prettytable.PrettyTable(self.headdata)
        # 在终端上显示
        # self.table.padding_width = 1
        self.dbdata = self.dateget.fetchall()
        # for i in self.dbdata:
        #     self.table.add_row([i[j] for j in self.headindex])
        # ini.displaytext = ini.displaytext + str(self.table) + '\n\n'
        # print(ini.displaytext)


class Email:
    def loging(self):
        print('>>> 登录email...')
        try:
            self.smtpObj = smtplib.SMTP()
            self.smtpObj.connect(ini.email['mail_host'], 25)
            self.smtpObj.login(ini.email['mail_user'], ini.email['mail_pwd'])
            print('>>> 登录email成功。')
        except:
            print('>>> email登录失败，请检查！')
            sys.exit()

    def emailtext(self, text):
        self.maintext = text

    def sendemail(self):
        # self.message = MIMEText(self.maintext, 'plain', 'utf-8')
        self.message = MIMEText(self.maintext, 'html', 'utf-8')
        temp_subject = db.ip + '-'
        if ini.config['timetype'] == 'raw_monitor':
            if ini.subject_overcrowding == 1:
                temp_subject += '【拥塞】'
            if ini.subject_sleep == 1:
                temp_subject += '【休眠】'
            if ini.subject_sleep_0 == 1:
                temp_subject += '【零流量】'
            if ini.subject_gps == 1:
                temp_subject += '【干扰】'
            if ini.subject_maxue == 1:
                temp_subject += '【maxue】'
        self.message['Subject'] = temp_subject + ini.subject
        self.message['From'] = ini.email['mail_user']
        if len(ini.email['receivers']) > 1:
            self.message['To'] = ';'.join(ini.email['receivers'])
        else:
            self.message['To'] = ini.email['receivers'][0]

        self.message["Accept-Language"] = "zh-CN"
        self.message["Accept-Charset"] = "ISO-8859-1,utf-8"

        self.smtpObj.sendmail(ini.email['mail_user'], ini.email['receivers'],
                              self.message.as_string())
        print('>>> email已发送！')


class Html:
    def __init__(self):
        self.MIMEtext = ''

    def head(self):
        self.MIMEtext += '''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>'''
        self.MIMEtext += ini.title
        self.MIMEtext += '''</title>
            </head>
            <body>'''

    def body(self, html_type, html_text):
        self.MIMEtext += '<%s><pre>%s</pre></%s>' % (html_type, html_text,
                                                     html_type)

    def table(self):
        self.MIMEtext += '''
         <table border = '1' cellspacing="0">
             <thead align = 'center' style = "background:#F2F2F2">
        <tr>
        '''
        for i in db.headdata:
            self.MIMEtext += '<td><b> '
            self.MIMEtext += i.upper()
            self.MIMEtext += '</b></td>'
            # 添加中文小区名和ip
            if db.kpirange[db.head_data_index[i]][1] == 'cellname':
                self.MIMEtext += '<td><b>cellname</b></td><td><b>ip</b></td>'
        self.MIMEtext += '''</tr></thead><tbody align = 'center'>'''
        for j in db.dbdata:
            self.MIMEtext += '<tr>'
            for k in db.headindex:
                self.MIMEtext += '<td>'
                if range(j[k], db.kpirange[k]):
                    self.MIMEtext += '<font color="#ff0000"><b>'
                if str(j[k]).count('.') == 1:
                    # self.MIMEtext += str(round(float(j[k]), 3))
                    try:
                        self.MIMEtext += str(round(float(j[k]), 3))
                    except:
                        self.MIMEtext += str(j[k])
                else:
                    self.MIMEtext += str(j[k])
                if range(j[k], db.kpirange[k]):
                    self.MIMEtext += '</b></font>'
                self.MIMEtext += '</td>'
                # 添加中文小区名和ip
                if db.kpirange[k][1] == 'cellname':
                    self.MIMEtext += '<td>'
                    try:
                        self.MIMEtext += ini.cellinfo_data[j[k]][2]
                    except:
                        self.MIMEtext += '-'
                    self.MIMEtext += '</td><td>'
                    try:
                        self.MIMEtext += ini.cellinfo_data[j[k]][1]
                    except:
                        self.MIMEtext += '-'
                    self.MIMEtext += '</td>'

            self.MIMEtext += '</tr>'

        self.MIMEtext += '</tbody></table>'

    def foot(self):
        self.MIMEtext += '</body></html>'


def range(value, list):
    try:
        if list[0] == '>':
            return float(value) > float(list[1])
        elif list[0] == '>=':
            return float(value) >= float(list[1])
        elif list[0] == '<':
            return float(value) < float(list[1])
        elif list[0] == '<=':
            return float(value) <= float(list[1])
        elif list[0] == '=':
            return float(value) == float(list[1])
        elif list[0] == '!=':
            return float(value) != float(list[1])
        elif list[0] == '':
            return 0
    except:
        pass


class Report:
    def __init__(self):
        # 如果改时段没有top小区，则显示 ‘无'
        self.topcelln = 0
        # 读取小区基础信息
        ini.cellinfo()

    def day_hour_report(self, type):
        # 获取常用kpi指标
        db.getdata(ini.mainsql)
        db.displaydata()
        html.head()
        html.body('h1', '一、概况')
        html.table()

        # 休眠小区
        db.getdata(ini.sleepingcell_sql)
        db.displaydata(datatype='sleepingcell')
        if len(db.dbdata) != 0:
            html.body('h2', '  休眠小区')
            html.table()

        # 休眠小区1
        db.getdata(ini.sleep_cell_16a_hour)
        db.displaydata(datatype='sleep_cell_16a')
        if len(db.dbdata) != 0:
            html.body('h2', '  休眠及零流量小区')
            html.table()

        # 最大激活用户数检测
        db.getdata(ini.maxue, timetype='top')
        db.displaydata(datatype='maxue')
        if len(db.dbdata) != 0:
            html.body('h2', '  最大激活用户数检测')

            if type == 'hour':
                html.body('h4', '   最近一小时最大激活用户数超过配置门限小区，请尽快处理！')
            elif type == 'day':
                html.body('h4', '   昨天最大激活用户数超过配置门限小区，请尽快处理！')
            elif type == 'raw':
                html.body('h4', '   最近15分钟最大激活用户数超过配置门限小区，请尽快处理！')
            html.table()
            self.topcelln += 1

        # 获取top小区指标
        if type == 'hour':
            html.body('h1', '二、最近一小时 TOP N 小区')
        elif type == 'day':
            html.body('h1', '二、昨天 TOP N 小区')
        elif type == 'raw':
            html.body('h1', '二、最近15分钟 TOP N 小区')

        # html.body('h2', '  1、VOLTE')
        # esrvcc成功率
        db.getdata(ini.top_volte_sql, timetype='top', counter='esrvcc切换失败次数ZB')
        db.displaydata(datatype='top_srvcc')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎  esrvcc成功率')
            html.table()
            self.topcelln += 1
        # qci1_erab建立成功率
        db.getdata(
            ini.top_volte_sql, timetype='top', counter='qci1_erab建立失败次数')
        db.displaydata(datatype='top_qci1connect')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ qci1_erab建立成功率')
            html.table()
            self.topcelln += 1

        # qci2_erab建立成功率
        db.getdata(
            ini.top_volte_sql, timetype='top', counter='qci2_erab建立失败次数')
        db.displaydata(datatype='top_qci2connect')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ qci2_erab建立成功率')
            html.table()
            self.topcelln += 1

        # QCI1掉线率
        db.getdata(ini.top_volte_sql, timetype='top', counter='QCI1掉线次数')
        db.displaydata(datatype='top_qci1drop')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ QCI1掉线率')
            html.table()
            self.topcelln += 1

        # html.body('h2', '  2、关键 KPI')
        # RRC连接建立成功率
        db.getdata(ini.top_kpi_sql, timetype='top', counter='RRC连接建立失败次数')
        db.displaydata(datatype='top_rrcconnect')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ RRC连接建立成功率')
            html.table()
            self.topcelln += 1

        # 切换请求次数QQ
        db.getdata(ini.top_kpi_sql, timetype='top', counter='切换失败次数QQ')
        db.displaydata(datatype='top_handover')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ 切换成功率QQ')
            html.table()
            self.topcelln += 1

        # ERAB建立成功率
        db.getdata(ini.top_kpi_sql, timetype='top', counter='ERAB建立失败数')
        db.displaydata(datatype='top_erabconnect')
        if len(db.dbdata) != 0:
            html.body('h3', '   ◎ ERAB建立成功率')
            html.table()
            self.topcelln += 1

        # 无线掉线率
        db.getdata(ini.top_kpi_sql, timetype='top', counter='无线掉线率分子')
        db.displaydata(datatype='top_radiodrop')
        if len(db.dbdata) != 0:
            html.body('h3', '    ◎ 无线掉线率')
            html.table()
            self.topcelln += 1

        # ERAB掉线率
        db.getdata(ini.top_kpi_sql, timetype='top', counter='ERAB掉线次数')
        db.displaydata(datatype='top_erabdrop')
        if len(db.dbdata) != 0:
            html.body('h3', '   ◎  ERAB掉线率')
            html.table()
            self.topcelln += 1

        if self.topcelln == 0:
            html.body('h3', '   ◎    无')

        # html结束
        html.foot()
        # 生成HTML文件
        # f_html = open(
        #     ''.join((ini.path, '/', 'HTML_TEMP', '/', db.ip, '-', ini.htmlname, '.html')),
        #     'w',
        #     encoding='utf-8')
        f_html = open(
            ''.join((ini.path, '/', 'HTML_TEMP', '/', ini.title, '.html')),
            'w',
            encoding='utf-8')
        f_html.write(html.MIMEtext)
        f_html.close()
        print('>>> 数据获取完成，已生成html报告!')

    def raw_monitor(self, type):
        html.head()
        html.body('h1', 'HI，最近15分钟存在KPI恶化明显小区，可能对整网指标影响较大，请尽快处理！')
        # GPS故障小区
        db.getdata(ini.alarm_sql)
        db.displaydata(datatype='alarm')
        if len(db.dbdata) != 0:
            html.body('h2', '   ◎  GPS故障基站，可能造成大面积干扰，请尽快处理！')
            html.table()
            self.topcelln += 1
            ini.subject_gps = 1
        # 休眠小区
        db.getdata(ini.sleepingcell_sql)
        db.displaydata(datatype='sleepingcell')
        if len(db.dbdata) != 0:
            html.body('h2', '   ◎  休眠小区')
            html.table()
            self.topcelln += 1
            ini.subject_sleep = 1
        # 休眠及零流量小区
        db.getdata(ini.sleep_cell_16a_raw)
        db.displaydata(datatype='sleep_cell_16a')
        if len(db.dbdata) != 0:
            html.body('h2', '   ◎  休眠及零流量小区')
            html.table()
            self.topcelln += 1
            ini.subject_sleep_0 = 1
        # 高拥塞小区
        db.getdata(
            ini.top_kpi_sql, timetype='top', counter='拥塞次数', threshold=500)
        db.displaydata(datatype='overcrowding')
        if len(db.dbdata) != 0:
            html.body('h2', '   ◎  高拥塞小区')

            # 保留高拥塞小区信息
            if ini.config['autodisabledpmmeasurement'] == '1':
                ini.autodisabledpmmeasurementdata = [temp_i[2] for temp_i in db.dbdata]
                html.body('h3', '<font color="#ff0000"><b>     !!!注意!!! 以下小区因拥塞对现网指标影响较大，'
                                '已尝试将RRC测量上报开关关闭!!!</b></font>')
                html.body('h3', '<font color="#ff0000"><b>       >>>请尽快处理并恢复测量开关！<<<</b></font>')
            html.table()
            self.topcelln += 1
            ini.subject_overcrowding = 1

        # 最大激活用户数检测
        db.getdata(ini.maxue, timetype='top')
        db.displaydata(datatype='maxue')
        if len(db.dbdata) != 0:
            html.body('h2', '   ◎  最大激活用户数检测：最近一个时段最大激活用户数超过配置门限，请尽快扩容！')
            html.table()
            self.topcelln += 1
            ini.subject_maxue = 1

        # html结束
        html.foot()
        # 生成HTML文件
        if self.topcelln != 0:
            # f_html = open(
            #     ''.join((ini.path, '/', 'HTML_TEMP', '/', ini.htmlname,
            #              '_monitor.html')),
            #     'w',
            #     encoding='utf-8')
            f_html = open(
                ''.join((ini.path, '/', 'HTML_TEMP', '/', ini.title, '.html')),
                'w',
                encoding='utf-8')
            f_html.write(html.MIMEtext)
            f_html.close()
            print('>>> 数据获取完成，已生成html报告!')
        else:
            print('>>> 该时段未存在恶化小区！')


if __name__ == '__main__':
    ini = Getini()
    ini.automonitor()
    for db_child in ini.db:
        db = Db(ini.db[db_child][0], ini.db[db_child][1], ini.db[db_child][2])
        html = Html()
        db.db_connect()
        if db.connectstate == 0:
            sys.exit()

        # 获取生成html文件名及邮件头名
        ini.title = '_'.join((ini.db[db_child][0], ini.cf.get('email', 'subject'), ini.htmlname))

        db_report = Report()

        report_type = {'day': db_report.day_hour_report,
                       'hour': db_report.day_hour_report,
                       'raw': db_report.day_hour_report,
                       'raw_monitor': db_report.raw_monitor}
        report_type[ini.config['timetype']](ini.config['timetype'])

        # 如果不存在恶化小区，则不发送邮件
        if ini.config['timetype'] == 'raw_monitor' and db_report.topcelln == 0:
            ini.main['actemail'] = '0'

        # 发送邮件
        if ini.main['actemail'] == '1':
            email = Email()
            email.loging()
            email.emailtext(html.MIMEtext)
            email.sendemail()
            email.smtpObj.close()

        # 关闭高拥塞小区测量
        if ini.autodisabledpmmeasurementdata != '':
            print('>>> 开始尝试关闭拥塞小区测量...')
            # 获取高拥塞小区，并转化成 {enbid：ip} 格式
            disabledpmmeasurement_list = {}
            for i in ini.autodisabledpmmeasurementdata:
                try:
                    if ini.cellinfo_data[i][1].count('.') == 3:
                        disabledpmmeasurement_list[ini.cellinfo_data[i][0][:6]] = ini.cellinfo_data[i][1]
                except:
                    print(''.join(('>>> 基础数据 cellinfo 中未存在ENBID:', i, ' ,请检查完善！')))
            # 如果所有小区都没有读取到ip，则退出
            if len(disabledpmmeasurement_list) == 0:
                sys.exit()
            # 生成批处理bat文件
            bat_path = ''.join((ini.path, '/CommisionTool/temp/DisabeledPMMeasurement_', ini.htmlname, '.bat'))
            with open(bat_path, 'w') as f_dm:
                for j in disabledpmmeasurement_list:
                    temp_text = ''.join(('call commission.bat -ne ',
                                         disabledpmmeasurement_list[j],
                                         ' -pw Nemuadmin:nemuuser -parameterfile disabled.xml |tee ./temp/',
                                         j, '.log'))
                    f_dm.write(temp_text)
                    f_dm.write('\n')
            # 修改运行文件夹为批处理文件所在目录，并执行批处理程序；
            os.chdir(''.join((ini.path, '/CommisionTool')))
            subprocess.call(bat_path)
            # 读取批处理程序运行结果，并生成csv记录表
            f_csv = ''.join((ini.path, '/HTML_TEMP/DisabeledPMMeasurementEnbList.csv'))
            if not os.path.exists(f_csv):
                f_csv_new = open(f_csv, 'w')
                f_csv_new.write('时间,enbid,ip,关闭测量情况\n')
                f_csv_new.close()
            with open(f_csv, 'a') as f_dml:
                for kk in disabledpmmeasurement_list:
                    f_dml.write("'")
                    f_dml.write(str(ini.htmlname))
                    f_dml.write(',')
                    f_dml.write(str(kk))
                    f_dml.write(',')
                    f_dml.write(str(disabledpmmeasurement_list[kk]))
                    f_dml.write(',')
                    with open(''.join((ini.path, '/CommisionTool/temp/', kk, '.log')), 'r') as f_log:
                        log_info = f_log.read()
                        if 'Successfully activated' in log_info:
                            f_dml.write('Successfully Disabeled PM Measurement')
                        elif ' Connection from 0.0.0.0 exists' in log_info:
                            f_dml.write('Connection refused')
                        elif 'Cannot connect to' in log_info:
                            f_dml.write('Connection Failed')
                        else:
                            f_dml.write('Other Failed')
                        f_dml.write('\n')
            print('>>> 完成！请到 /HTML_TEMP/DisabeledPMMeasurementEnbList.csv 检查运行结果.')
