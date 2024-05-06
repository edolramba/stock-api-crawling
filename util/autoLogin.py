import win32com.client
from pywinauto import application
import os
import time
from common.importConfig import *

class autoLogin:
    
    def __init__(self):
        self.connect(reconnect=False)
        
    def kill_client(self):
        print("########## 기존 CYBOS 프로세스 강제 종료")
        os.system('taskkill /IM ncStarter* /F /T')
        os.system('taskkill /IM CpStart* /F /T')
        os.system('taskkill /IM DibServer* /F /T')
        os.system('wmic process where "name like \'%ncStarter%\'" call terminate')
        os.system('wmic process where "name like \'%CpStart%\'" call terminate')
        os.system('wmic process where "name like \'%DibServer%\'" call terminate')

    def connect(self, reconnect=True):
        # 재연결이라면 기존 연결을 강제로 kill
        if reconnect:
            try:
                self.kill_client()
            except Exception as e:
                pass
                
        CpCybos = win32com.client.Dispatch("CpUtil.CpCybos")
        
        # 접속이 되어있으면 패스, 접속이 안되어 있으면 접속한다.
        if CpCybos.IsConnect:
            print('already connected.')
        else:
            importConf = importConfig()
            
            id = importConf.select_section("Creon")["id"]
            pwd = importConf.select_section("Creon")["pwd"]
            pwdcert = importConf.select_section("Creon")["pwdcert"]

            app = application.Application()
            app.start(
                        'C:\Daishin\Starter\\ncStarter.exe /prj:cp /id:{id} /pwd:{pwd} /pwdcert:{pwdcert} /autostart'.format(
                            id=id, pwd=pwd, pwdcert=pwdcert)
                    )
            # 연결 될때까지 무한루프
            while True:
                if CpCybos.IsConnect:
                    break
                time.sleep(1)

            print('Auto Login Success')
            
        return CpCybos

