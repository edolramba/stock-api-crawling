import configparser
import os

class importConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        # config.ini 파일의 경로를 지정
        self.config.read(os.path.join('C:\\Dev\\stock-api-crawling\\config', 'config.ini'))

    def select_section(self, section):
        # 각 섹션에 맞는 값을 딕셔너리로 반환
        if section == "MONGODB":
            host = self.config[section]['host']
            port = self.config[section]['port']
            return {"host": host, "port": port}
        
        elif section == "Creon":
            # id, pwd, pwdcert 값을 올바르게 할당
            id = self.config[section]['id']
            pwd = self.config[section]['pwd']
            pwdcert = self.config[section]['pwdcert']
            return {"id": id, "pwd": pwd, "pwdcert": pwdcert}
        
        elif section == "TELEGRAM":
            chat_id = self.config[section]['chat_id']
            self_token = self.config[section]['self_token']
            self_chat_id = self.config[section]['self_chat_id']
            return {"chat_id": chat_id, "self_token": self_token, "self_chat_id": self_chat_id}
        
        else:
            print("Not yet setting section")
            return None
