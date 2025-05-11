@ECHO OFF
:: UTF-8 코드 페이지 설정
chcp 65001 > nul

:: 관리자 권한 확인 및 요청
NET FILE >nul 2>&1
if '%errorlevel%' NEQ '0' (
    ECHO 관리자 권한으로 실행 중이 아닙니다. 관리자 권한으로 다시 실행합니다.
    powershell -Command "Start-Process cmd.exe -ArgumentList '/c C:\Dev\stock-api-crawling\script\startDataCrawler.bat' -Verb runAs"
    EXIT /B
)

title startDataCrawler Start

REM 환경 변수 설정
set PATH=C:\Users\MYUNGWOO\Envs\stock-api-crawling\Scripts;%PATH%
set PYTHONHOME=C:\Users\MYUNGWOO\Envs\stock-api-crawling
set PYTHONPATH=C:\Dev\stock-api-crawling

REM 작업 디렉터리 변경
cd /d "C:\Dev\stock-api-crawling"
ECHO 작업 디렉터리: %cd%

call workon stock-api-crawling
ECHO 가상 환경 활성화 완료

REM 로그 파일 경로 설정
set log_dir=C:\Dev\stock-api-crawling\logs
if not exist "%log_dir%" mkdir "%log_dir%"
set log_file=%log_dir%\dataCrawler_%date:~-10,4%-%date:~-5,2%-%date:~-2,2%.log

REM Python 실행
python dataCrawler.py >> %log_file% 2>&1
ECHO Python 스크립트 실행 완료 >> %log_file%

ECHO 작업 완료
exit
