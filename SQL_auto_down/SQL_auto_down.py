# -*- coding: utf-8 -*-
import configparser, cx_Oracle, os, sys, datetime, time

# 获取配置文件

##############################################################################
print("""

    Welcome to use tools!
    Version : 1.2.1
    Author : linxuteng
    E_mail : lxuteng@163.com



""")  ##
time.sleep(1)  ##
exetime = int(time.strftime('%Y%m%d', time.localtime(time.time())))  ##
if exetime > 20180101:  ##
    print('试用版本已过期，请联系作者！')  ##
    input()  ##
    sys.exit()  ##
    ##
print('>>> starting!')  ##
print('\n')  ##
time.sleep(1)  ##
##
##
##################################################################################

inipath = os.path.split(os.path.abspath(sys.argv[0]))[0]
cf = configparser.ConfigParser()
cf.read(''.join((inipath, '\\', 'config.ini')), encoding='utf-8-SIG')

day = cf.get('config', 'day').split(',')
if '' in day:
    # day = [str(int(time.strftime('%Y%m%d', time.localtime(time.time())))-1)]
    day = [
        (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')]
star_hour = cf.get('config', 'star_hour')
end_hour = cf.get('config', 'end_hour')
SQL_path = cf.get('config', 'SQL_path')

# 获取数据库名称及账号密码
db_name_list = cf.options('db')
if len(db_name_list) == 0:
    print('未设置数据库，请检查！')
    exit()
else:
    db_list = {}
    for db_i in db_name_list:
        db_list_i = cf.get('db', db_i).split(',')
        if db_list_i[0] in db_list:
            print(db_list_i[0], ' 名称重复，请检查!')
            exit()
        else:
            db_list[db_list_i[0]] = (db_list_i[1], db_list_i[2])

# 获取SQL脚本相关设置
SQL_name_list = cf.options('SQL_name')
if len(SQL_name_list) == 0:
    print('未设置SQL脚本相关查询、存放参数，请检查！')
    exit()
else:
    SQL_list = {}
    for sql_i in SQL_name_list:
        sql_list_i = cf.get('SQL_name', sql_i).split(',')
        x = 0
        y = 0
        while x == 0:
            if sql_list_i[0] + '{' + str(y) + '}' in SQL_list:
                y += 1
            else:
                x = 1
        SQL_list[sql_list_i[0] + '{' + str(y) + '}'] = (
        sql_list_i[1], sql_list_i[2], sql_list_i[3])


# 获取本地文件及文件夹
def local_dir(path):
    local_dir_list = []
    local_file_list = []
    for i in os.listdir(path):
        if os.path.isdir('\\'.join((path, i))):
            local_dir_list.append(i)
        else:
            local_file_list.append(i)
    return (local_dir_list, local_file_list)


# main
for db_ob in db_list:
    print('连接数据库：', db_ob, '...')
    conn = cx_Oracle.connect(db_list[db_ob][0], db_list[db_ob][1], db_ob)
    try:
        conn = cx_Oracle.connect(db_list[db_ob][0], db_list[db_ob][1], db_ob)
        print('连接成功!')
    except:
        print('无法连接数据库：', db_ob, ',请检查网络或数据库设置!')
        continue
    c = conn.cursor()
    for SQL_list_ob in SQL_list:
        for day_ob in day:
            if SQL_list[SQL_list_ob][0] == 'day':
                star_time = day_ob
                end_time = day_ob
            elif SQL_list[SQL_list_ob][0] == 'hour':
                star_time = ''.join((day_ob, star_hour))
                end_time = ''.join((day_ob, end_hour))
            elif SQL_list[SQL_list_ob][0] == 'raw':
                star_time = ''.join((day_ob, star_hour, '00'))
                end_time = ''.join((day_ob, end_hour, '59'))
            elif SQL_list[SQL_list_ob][0] == 'para':
                pass
            else:
                print('取数粒度设置错误，请检查！！')
                exit()
            print(''.join(('尝试打开SQL脚本:', SQL_list_ob[:SQL_list_ob.find('{')],
                           '.sql ...')))
            try:
                ff = open(''.join((SQL_path, '\\',
                                   SQL_list_ob[:SQL_list_ob.find('{')],
                                   '.sql')), 'r')
                print(
                    ''.join((SQL_list_ob[:SQL_list_ob.find('{')], '.sql 已打开。')))
            except:
                print('SQL脚本：', SQL_list_ob[:SQL_list_ob.find('{')],
                      ' 不存在，请检查！')
                continue
            if SQL_list[SQL_list_ob][0] == 'para':
                try:
                    sql_ob = ff.read()
                except:
                    print('SQL脚本出错，请检查！')
                    continue
            else:
                try:
                    sql_ob = ff.read().replace('&1', star_time).replace('&2',
                                                                        end_time)
                except:
                    print('SQL日期设置异常，请检查！')
                    continue
            print('查询开始,请稍后...')
            try:
                x = c.execute(sql_ob)
                print('查询完成！')
            except:
                print('查询出错，请检查SQL脚本！！')
                continue
            # if len(list(x.fetchall())) == 0:
            #     print('未获取到数据！')
            # else:
            #     print('查询结束，共获取数据：',len(list(x.fetchall())),'\n')
            print('开始导出，请稍后...')

            local_dir_list = local_dir(SQL_list[SQL_list_ob][1])[1]
            if SQL_list[SQL_list_ob][2][0] == '$':
                name_down = ''.join(
                    (SQL_list[SQL_list_ob][2][1:], '_', db_ob, '.csv'))
                f = open(''.join((SQL_list[SQL_list_ob][1], '\\', name_down)),
                         'a', encoding='utf-8-SIG')
                if name_down not in local_dir_list:
                    for m in x.description:
                        f.write(m[0])
                        f.write(',')
                    f.write('\n')
            else:
                l = 0
                k = 0
                while k == 0:
                    if l == 0:
                        if ''.join((SQL_list[SQL_list_ob][2], '_', day_ob, '_',
                                    db_ob, '.csv')) in local_dir_list:
                            l += 1
                        else:
                            k = 1
                    else:
                        if ''.join((SQL_list[SQL_list_ob][2], '_', day_ob, '_',
                                    db_ob, '(', str(l),
                                    ').csv')) in local_dir_list:
                            l += 1
                        else:
                            k = 1

                    if l == 0:
                        name_down = ''.join((SQL_list[SQL_list_ob][2], '_',
                                             day_ob, '_', db_ob, '.csv'))
                    else:
                        name_down = ''.join((SQL_list[SQL_list_ob][2], '_',
                                             day_ob, '_', db_ob, '(', str(l),
                                             ').csv'))
                    f = open(
                        ''.join((SQL_list[SQL_list_ob][1], '\\', name_down)),
                        'w', encoding='utf-8-SIG')

                    for m in x.description:
                        f.write(m[0])
                        f.write(',')
                    f.write('\n')

            for i in x.fetchall():
                for j in i:
                    f.write(str(j))
                    f.write(',')
                f.write('\n')
            f.close()
            print(name_down, ' 导出完成！')
            print('*' * 32)
