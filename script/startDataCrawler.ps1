# 관리자 권한 확인
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Output "관리자 권한으로 다시 실행합니다."
    Start-Process -FilePath "powershell.exe" -ArgumentList "-File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# 작업 디렉터리 설정
Set-Location "C:\Dev\stock-api-crawling"

# 환경 변수 설정
$env:PATH = "C:\Users\MYUNGWOO\Envs\stock-api-crawling\Scripts;" + $env:PATH
$env:PYTHONHOME = "C:\Users\MYUNGWOO\Envs\stock-api-crawling"
$env:PYTHONPATH = "C:\Dev\stock-api-crawling"

# 로그 디렉터리 설정
$logDir = "C:\Dev\stock-api-crawling\log"
if (!(Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir
}
$logFile = "$logDir\dataCrawler_$(Get-Date -Format 'yyyy-MM-dd').log"

# Python 스크립트 실행
python dataCrawler.py >> $logFile 2>&1

Write-Output "작업 완료"
