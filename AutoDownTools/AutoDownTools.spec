# -*- mode: python -*-

block_cipher = None


a = Analysis(['AutoDownTools.py'],
             pathex=['C:\\Users\\lxute\\AppData\\Local\\Programs\\Python\\Python35\\Lib\\site-packages', 'C:\\Users\\lxute\\AppData\\Local\\Programs\\Python\\Python35\\Lib\\site-packages\\cryptography', 'D:\\python\\code\\AutoDownTools'],
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
          a.binaries,
          a.zipfiles,
          a.datas,
          name='AutoDownTools',
          debug=False,
          strip=False,
          upx=True,
          console=True , icon='D:\\python\\2_software\\icon\\Thunderbolt.ico')
