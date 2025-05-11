import configparser
import os
import subprocess
import time
import pymongo
import gc
from pymongo import monitoring
from common.loggerConfig import setup_logger

log = setup_logger()

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

    def ensure_mongodb_running(self):
        try:
            # MongoDB 서비스 상태 확인
            status = subprocess.run(['sc', 'query', 'MongoDB'], capture_output=True, text=True)
            
            if "RUNNING" not in status.stdout:
                # 서비스 재시작
                subprocess.run(['sc', 'stop', 'MongoDB'], check=True)
                time.sleep(5)
                subprocess.run(['sc', 'start', 'MongoDB'], check=True)
                time.sleep(10)  # 서비스 시작 대기
                
            # 연결 테스트
            test_client = MongoDBHandler()
            test_client._client.admin.command('ping')
            return True
        except Exception as e:
            log.error(f"MongoDB 서비스 재시작 실패: {e}")
            return False

    def batch_update(self, operations, batch_size=1000):
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            try:
                self.db_handler._client[self.db_name][code['종목코드']].bulk_write(batch, ordered=False)
            except pymongo.errors.AutoReconnect:
                if not self.ensure_mongodb_running():
                    raise
            finally:
                gc.collect()

import threading

class CommandLogger(monitoring.CommandListener):
    def __init__(self, logger):
        self.log = logger
        self.last_log_time = {}
        self.log_interval = 30  # 30초
        self.lock = threading.Lock()

    def _should_log(self, key):
        now = time.time()
        with self.lock:
            last = self.last_log_time.get(key, 0)
            if now - last > self.log_interval:
                self.last_log_time[key] = now
                return True
            return False

    def started(self, event):
        key = f"started:{event.command_name}"
        if self._should_log(key):
            self.log.info(f"Command {event.command_name} started on {event.database_name}")
        
    def succeeded(self, event):
        key = f"succeeded:{event.command_name}"
        if self._should_log(key):
            self.log.info(f"Command {event.command_name} succeeded on {event.database_name}")
        
    def failed(self, event):
        key = f"failed:{event.command_name}:{getattr(event, 'failure', '')}"
        if self._should_log(key):
            self.log.error(f"Command {event.command_name} failed on {event.database_name} with error: {getattr(event, 'failure', '')}")

# MongoDB 모니터링 등록
monitoring.register(CommandLogger(log))
