import paramiko
import time
import os
import sys
import configparser


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

# 获取配置文件
inipath = os.path.split(os.path.abspath(sys.argv[0]))[0]
cf = configparser.ConfigParser()

cf.read(''.join((inipath, '\\', 'config.ini')), encoding='utf-8-SIG')
enb_ip_list = cf.get('config', 'ip').split(',')
enb_usr = cf.get('config', 'usr')
enb_pwr = cf.get('config', 'pwr')
timesleep = int(cf.get('config', 'timesleep'))
send_cmd = cf.get('config', 'cmd')
run_stata = open('/'.join(
    (os.path.split(os.path.abspath(sys.argv[0]))[0], 'run_stata.csv')), 'w')
for enb_ip in enb_ip_list:
    run_stata.write(enb_ip)
    run_stata.write(',')
    try:
        # 连接IP
        print('>>> 正在连接 ', enb_ip, '...')
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(enb_ip, 22, enb_usr, enb_pwr)
        print('>>> 连接成功')
    except:
        run_stata.write('connect_fail')
        run_stata.write('\n')
        print('>>>', enb_ip, ' 连接异常，请检查！')
        print('-' * 8)
        continue
    try:
        print('>>> 发送指令...')
        # 发送命令
        stdin_0, stdout_0, stderr_0 = ssh.exec_command(send_cmd, timeout=5)
        run_stata.write('cmd_send_success')
        run_stata.write('\n')
        print('>>> 指令发送成功！')
    except:
        print('>>> 指令执行出错，请检查!')
        print('-' * 8)
        run_stata.write('cmd_send_fail')
        run_stata.write('\n')
        continue
    print('-' * 8)
    time.sleep(timesleep)
run_stata.close
print('-' * 8)
print('all done!')
print('-' * 8)
