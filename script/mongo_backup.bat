@echo off
setlocal enabledelayedexpansion

rem 날짜 계산
for /f "tokens=1,2,3 delims=- " %%A in ("%DATE%") do (
    set YY=%%A
    set MM=%%B
    set DD=%%C
)

rem 날짜 포맷 설정
set yyyymmdd=%YY%%MM%%DD%

rem 로그 파일 이름 설정
set logFile="D:\Program Files\MongoDB\Server\4.4\backup_log_%yyyymmdd%.txt"

rem Create and write initial message to the log file
echo %DATE% %TIME%: [INFO] Starting MongoDB backup script... > %logFile%

rem 6일 전 날짜 계산
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).AddDays(-6).ToString('yyyyMMdd')"') do set date6daysago=%%i

rem 변수 설정
set dumpPath="D:\Program Files\MongoDB\Server\4.4\dump\%yyyymmdd%"
set DIRORG="D:\Program Files\MongoDB\Server\4.4\dump\%yyyymmdd%"
set BAKPATH="D:\Program Files\MongoDB\Server\4.4\dump"

rem 변수 값 에코
echo %DATE% %TIME%: [INFO] Calculated date 6 days ago: %date6daysago% >> %logFile%
echo %DATE% %TIME%: [INFO] yyyymmdd: %yyyymmdd% >> %logFile%
echo %DATE% %TIME%: [INFO] dumpPath: %dumpPath% >> %logFile%
echo %DATE% %TIME%: [INFO] DIRORG: %DIRORG% >> %logFile%
echo %DATE% %TIME%: [INFO] BAKPATH: %BAKPATH% >> %logFile%

rem 백업 경로에 날짜 폴더가 없으면 생성
if not exist %dumpPath% (
    echo %DATE% %TIME%: [INFO] Creating dump directory: %dumpPath% >> %logFile%
    mkdir %dumpPath%
)

rem 현재 파일 목록 확인
echo %DATE% %TIME%: [DEBUG] Listing files in BAKPATH: %BAKPATH% >> %logFile%
dir %BAKPATH% >> %logFile%

rem 백업이 이미 존재하는지 확인
echo %DATE% %TIME%: [DEBUG] Checking if backup zip file exists: %dumpPath%.zip >> %logFile%
if exist %dumpPath%.zip (
    echo %DATE% %TIME%: [INFO] Backup for %yyyymmdd% already exists. Skipping backup. >> %logFile%
    echo %DATE% %TIME%: [INFO] BAKPATH22: %dumpPath%.zip >> %logFile%
    rem 덤프 폴더가 존재하면 삭제
    if exist %dumpPath% (
        echo %DATE% %TIME%: [INFO] Deleting leftover dump folder... >> %logFile%
        rmdir /s /q %dumpPath%
    )
    goto delete_old_backups
) else (
    echo %DATE% %TIME%: [DEBUG] Entering else block... >> %logFile%
    echo %DATE% %TIME%: [INFO] Starting MongoDB dump... >> %logFile%
    echo %DATE% %TIME%: [DEBUG] Changing directory to MongoDB tools... >> %logFile%
    cd "C:\Program Files\MongoDB\Tools\100\bin"
    
    rem 현재 디렉토리 확인
    echo %DATE% %TIME%: [DEBUG] Current directory: %CD% >> %logFile%
    
    rem 디버깅을 위한 명령어 출력
    echo %DATE% %TIME%: [DEBUG] Executing mongodump command... >> %logFile%
    echo mongodump --out=%dumpPath% --host=127.0.0.1 --port=27017 >> %logFile%
    
    mongodump --out=%dumpPath% --host=127.0.0.1 --port=27017 >> %logFile% 2>&1
    echo %DATE% %TIME%: [DEBUG] mongodump command executed. Checking error level... >> %logFile%
    if %errorlevel% neq 0 (
        echo %DATE% %TIME%: [ERROR] mongodump failed with errorlevel %errorlevel%. >> %logFile%
        exit /b 1
    ) else (
        echo %DATE% %TIME%: [INFO] mongodump completed successfully. >> %logFile%
    )

    echo %DATE% %TIME%: [INFO] Compressing dump folder... >> %logFile%
    "C:\Program Files\Bandizip\Bandizip.exe" bc %DIRORG% >> %logFile% 2>&1
    if %errorlevel% neq 0 (
        echo %DATE% %TIME%: [ERROR] Bandizip compression failed with errorlevel %errorlevel%. >> %logFile%
        exit /b 1
    ) else (
        echo %DATE% %TIME%: [INFO] Bandizip compression completed successfully. >> %logFile%
    )

    rem 백업 폴더의 크기 계산
    set backupSize=0
    for /f "tokens=3" %%a in ('dir /s /-c %DIRORG% ^| findstr /c:" 파일"') do set backupSize=%%a

    echo %DATE% %TIME%: [INFO] Logging the backup size... >> %logFile%
    rem 결과를 로그에 기록
    echo %DATE% %TIME%: %backupSize% KB >> %logFile%

    rem 로그 기록이 성공했는지 확인
    if exist %logFile% (
        echo %DATE% %TIME%: [INFO] Log entry added successfully. >> %logFile%
    ) else (
        echo %DATE% %TIME%: [ERROR] Failed to add log entry. >> %logFile%
    )

    echo %DATE% %TIME%: [INFO] Deleting dump folder after compression... >> %logFile%
    rem 압축 후 덤프 폴더 삭제
    rmdir /s /q %dumpPath%
)

:delete_old_backups
echo %DATE% %TIME%: [INFO] Deleting old backups... >> %logFile%

rem 6일 이전의 오래된 .zip 백업 파일 삭제
for %%f in (%BAKPATH%\*.zip) do (
    set "filepath=%%~ff"
    setlocal enabledelayedexpansion
    echo %DATE% %TIME%: [INFO] Checking file: !filepath! >> %logFile%
    set "filename=%%~nf"
    echo %DATE% %TIME%: [INFO] Extracted filename: !filename! >> %logFile%
    if !filename! LSS %date6daysago% (
        echo %DATE% %TIME%: [INFO] Deleting file: !filepath! >> %logFile%
        del "!filepath!"
        if exist "!filepath!" (
            echo %DATE% %TIME%: [ERROR] Failed to delete file: !filepath! >> %logFile%
        ) else (
            echo %DATE% %TIME%: [INFO] Deleted file: !filepath! >> %logFile%
        )
    )
    endlocal
)

rem 6일 이전의 오래된 폴더 삭제
for /d %%i in (%BAKPATH%\*) do (
    set "folderpath=%%~fi"
    setlocal enabledelayedexpansion
    echo %DATE% %TIME%: [INFO] Checking folder: !folderpath! >> %logFile%
    set "foldername=%%~ni"
    echo %DATE% %TIME%: [INFO] Extracted foldername: !foldername! >> %logFile%
    if !foldername! LSS %date6daysago% (
        echo %DATE% %TIME%: [INFO] Deleting folder: !folderpath! >> %logFile%
        rmdir /s /q "!folderpath!"
        if exist "!folderpath!" (
            echo %DATE% %TIME%: [ERROR] Failed to delete folder: !folderpath! >> %logFile%
        ) else (
            echo %DATE% %TIME%: [INFO] Deleted folder: !folderpath! >> %logFile%
        )
    ) else (
        echo %DATE% %TIME%: [INFO] No folder to delete: !folderpath! >> %logFile%
    )
    endlocal
)

echo %DATE% %TIME%: [INFO] Backup script completed. >> %logFile%

rem 디버깅: 폴더가 삭제되었는지 확인
if not exist %BAKPATH%\%yyyymmdd% (
    echo %DATE% %TIME%: [INFO] Folder deleted successfully. >> %logFile%
) else (
    echo %DATE% %TIME%: [ERROR] Failed to delete folder: %BAKPATH%\%yyyymmdd% >> %logFile%
)

rem 로그 파일이 생성되었는지 확인
if exist %logFile% (
    echo %DATE% %TIME%: [INFO] Log file already exists or created successfully. >> %logFile%
) else (
    echo %DATE% %TIME%: [ERROR] Failed to create log file. >> %logFile%
)

goto :eof

exit