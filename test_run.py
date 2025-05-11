import subprocess

# workon 명령어를 사용해 가상환경 활성화 후 test_dataCrawler.py 실행
command = 'workon stock-api-crawling && python test_dataCrawler.py'

# 명령어 실행
subprocess.run(command, shell=True)