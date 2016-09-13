# -*- mode: python -*-

block_cipher = None


a = Analysis(['AutoMonitor.py'],
             pathex=['C:\\Users\\lxute\\AppData\\Local\\Programs\\Python\\Python35\\Lib\\site-packages', 'D:\\python\\code\\AutoMonitor'],
             binaries=None,
             datas=None,
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries + [('oraociei11.dll','C:\\instantclient_11_2\\oraociei11.dll','BINARY')],
          a.zipfiles,
          a.datas,
          name='AutoMonitor',
          debug=False,
          strip=False,
          upx=False,
          console=True , icon='D:\\python\\2_software\\icon\\Thunderbolt.ico')
