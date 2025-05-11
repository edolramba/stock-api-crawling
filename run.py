import os
import sys
import ctypes
import subprocess

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

if not is_admin():
    # 관리자 권한으로 재실행 (UAC 팝업)
    ctypes.windll.shell32.ShellExecuteW(
        None, "runas", sys.executable, " ".join(sys.argv), None, 1)
    sys.exit()

# workon 명령어를 사용해 가상환경 활성화 후 dataCrawler.py 실행
command = 'workon stock-api-crawling && python dataCrawler.py'

# 명령어 실행
subprocess.run(command, shell=True)

print("==== 데이터 수집 종료 ====")

os._exit(0)