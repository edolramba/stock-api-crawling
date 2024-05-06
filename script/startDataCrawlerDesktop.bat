@echo off
:: 관리자 권한으로 실행되지 않았다면, 관리자 권한으로 다시 실행
NET SESSION >nul 2>&1
if %errorLevel% == 1 (
    echo Requesting administrative privileges...
    goto UACPrompt
) else ( goto gotAdmin )

:UACPrompt
echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
set params = %*:"=""
echo UAC.ShellExecute "%~s0", "%params%", "", "runas", 1 >> "%temp%\getadmin.vbs"
"%temp%\getadmin.vbs"
del "%temp%\getadmin.vbs"
exit /B

:gotAdmin
pushd "%CD%"
CD /D "%~dp0"

title startDataCrawlerDesktop Start

cd C:\Dev\stock-api-crawling
call workon stock-api-crawling

python startDataCrawlerDesktop.py