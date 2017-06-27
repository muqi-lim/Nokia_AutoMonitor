__author__= 'linxuteng'



import xml.etree.ElementTree as ET
# import lxml.etree as ET
import time, configparser, os, sys


##############################################################################
print("""
--------------------------------
    Welcome to use tools!
    Version : 1.0.0
    Author : linxuteng
    E_mail : lxuteng@live.cn
--------------------------------
""")
print('\n')
time.sleep(1)
exetime = int(time.strftime('%Y%m%d', time.localtime(time.time())))
if exetime > 20180101:
    print('\n')
    print('-' * 64)
    print('  试用版本已过期，请联系作者！')
    print('-' * 64)
    print('\n')
    input()
    sys.exit()
print('\n')
print('-'*36)
print('      >>>   starting   <<<')
print('-'*36)
print('\n\n')
time.sleep(1)

###############################################################################


start = time.time()

# 获取配置文件
inipath = os.path.split(os.path.abspath(sys.argv[0]))[0]
cf = configparser.ConfigParser()
cf.read(''.join((inipath, '\\', 'config.ini')), encoding='utf-8-SIG')

# 需处理xml目录
path = cf.get('config', 'path')
if path == '':
    path = inipath

# 处理文件类型
parse_type = cf.get('config', 'parse_type').split(',')

# 处理结果存放位置
res_path = cf.get('config', 'res_path')
if res_path == '':
    res_path = inipath

object_head = cf.get('config', 'object_head')
if object_head == '':
    object_head = 'dis_split'

print('-' * 32)

# 定义函数，取各个object ID
def object_name(object_str):
    if object_head == 'dis_split':
        return {'object_id': object_str}
    else:
        return dict([i.split('-') for i in object_str.split('/')])


#进度条
def progress(len_,count_):
    bar_len = 24
    hashes = '|' * int(count_/len_ * bar_len)
    spaces = '_' * (bar_len - len(hashes))
    sys.stdout.write("\r%s %s %d%%" % (str(count_), hashes + spaces, int(count_ / len_ * 100)))
    sys.stdout.flush()

# 获取版本号
def getversion(line_one):
    for line_next in line_one:
        if line_next.tag[line_next.tag.find('}') + 1:] == 'managedObject':
            return line_next.attrib['version']
        else:
            try:
                version = getversion(line_next)
            except:
                pass
    return version

def ns_name(mob):
    return ''.join((ns, mob))

# 写入结果文档
def write_file():
    for i in parse_res:
        f = open(''.join((res_path, '\\', i, '.csv')), 'a')
        for j in parse_res[i]:
            object_n = object_name(j)
            if i not in parse_object_head:
                parse_object_head[i] = object_n.keys()
            for object_name_1 in parse_object_head[i]:
                f.write(object_n[object_name_1])
                f.write(',')
            f.write(parse_version[j])
            f.write(',')
            for k in parse_head[i]:
                try:
                    if isinstance(parse_res[i][j][k], list):
                        f.write(' '.join(parse_res[i][j][k]))
                    else:
                        f.write(str(parse_res[i][j][k]))
                except:
                    pass
                f.write(',')
            f.write('\n')
            # f.close()

# 获取路径下所有文件
file_list = []
print('>>> 开始获取文件，请稍后...')
for root, dirs, files in os.walk(path):
    for file in files:
        for type_i in parse_type:
            if type_i in file and '.xml' in file:
                file_list.append(os.path.join(root, file))
if len(file_list) != 0:
    print('>>> 获取到文件：', len(file_list))
else:
    print('>>> 未获取到文件，请检查！！')
    sys.exit()

print('>>> 开始解码,请稍后...')

timerun = 0


errorfile = []
parse_head = {}
parse_object_head = {}
parse_res = {}
parse_version = {}
file_i = 0
len__ = len(file_list)
for file in file_list:
    file_i += 1

    timerun += 1
    progress(len__,timerun)
    try:
        tree = ET.parse(file)
    except:
        errorfile.append(file)
        continue

    root = tree.getroot()
    ns = root.tag[:root.tag.find('}') + 1]
    # parse_res = {}
    version = getversion(root)
    # 解析scfc
    for mob_one in tree.iter(tag=ns_name('managedObject')):
        # 判断class是否在结果列表及表头里边里面
        if mob_one.attrib['class'] not in parse_res:
            parse_res[mob_one.attrib['class']] = {}
        if mob_one.attrib['class'] not in parse_head:
            parse_head[mob_one.attrib['class']] = []
        if mob_one.attrib['distName'] not in parse_res[mob_one.attrib['class']]:
            parse_res[mob_one.attrib['class']][mob_one.attrib['distName']] = {}
        if mob_one.attrib['distName'] not in parse_version:
            parse_version[mob_one.attrib['distName']] = mob_one.attrib['version']
        # 解析下一级
        for mob_two in mob_one:
            if mob_two.tag == ns_name('p'):
                if mob_two.attrib['name'] not in parse_res[mob_one.attrib['class']][mob_one.attrib['distName']]:
                    parse_res[mob_one.attrib['class']][mob_one.attrib['distName']][
                        mob_two.attrib['name']] = mob_two.text
                if mob_two.attrib['name'] not in parse_head[mob_one.attrib['class']]:
                    parse_head[mob_one.attrib['class']].append(mob_two.attrib['name'])
            elif mob_two.tag == ns_name('list'):
                item_num = 1
                for mob_three in mob_two:
                    if mob_three.tag == ns_name('p'):
                        if mob_two.attrib['name'] not in parse_res[mob_one.attrib['class']][mob_one.attrib['distName']]:
                            parse_res[mob_one.attrib['class']][mob_one.attrib['distName']][mob_two.attrib['name']] = []
                        if mob_two.attrib['name'] not in parse_head[mob_one.attrib['class']]:
                            parse_head[mob_one.attrib['class']].append(mob_two.attrib['name'])
                        parse_res[mob_one.attrib['class']][mob_one.attrib['distName']][mob_two.attrib['name']].append(
                            mob_three.text)
                    elif mob_three.tag == ns_name('item'):
                        if 'List_' + mob_one.attrib['class'] not in parse_res:
                            parse_res['List_' + mob_one.attrib['class']] = {}
                        if 'List_' + mob_one.attrib['class'] not in parse_head:
                            parse_head['List_' + mob_one.attrib['class']] = []
                        if mob_one.attrib['distName'] not in parse_res['List_' + mob_one.attrib['class']]:
                            parse_res['List_' + mob_one.attrib['class']][mob_one.attrib['distName']] = {}
                        for mob_four in mob_three:
                            if mob_two.attrib['name'] + '.' + str(item_num) + '.' + mob_four.attrib['name'] not in \
                                    parse_res['List_' + mob_one.attrib['class']][mob_one.attrib['distName']]:
                                parse_res['List_' + mob_one.attrib['class']][mob_one.attrib['distName']][
                                    mob_two.attrib['name'] + '.' + str(item_num) + '.' + mob_four.attrib[
                                        'name']] = mob_four.text
                            if mob_two.attrib['name'] + '.' + str(item_num) + '.' + mob_four.attrib['name'] not in \
                                    parse_head['List_' + mob_one.attrib['class']]:
                                parse_head['List_' + mob_one.attrib['class']].append(
                                    mob_two.attrib['name'] + '.' + str(item_num) + '.' + mob_four.attrib['name'])
                        item_num += 1
            else:
                print('暂不支持解码')
        if file_i == 80:
            write_file()
            parse_res = {}
            parse_version = {}
            file_i = 0

write_file()

# 写入表头
for i in parse_head:
    f = open(''.join((res_path, '\\', i, '.csv')), 'r')
    ff = f.readlines()
    f.close()
    head = []
    for m in parse_object_head[i]:
        head.append(m)
    head.append('version')
    for j in parse_head[i]:
        head.append(j)
    f = open(''.join((res_path, '\\', i, '.csv')), 'w')
    f.write(','.join(head))
    f.write('\n')
    f.writelines(ff)
    f.close()

end = time.time()

print('\n>>> 完成！')
#print('>>> 历时：', int(end - start), 's')
print('>>> 历时：', time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime(end - start)))
print('-' * 32)
if len(errorfile) == 0:
    pass
else:
    print('异常文件：')
    for error_i in errorfile:
        print(error_i)
        print('\n')
    print('-' * 32)