from common.importConfig import *
import pymongo
from pymongo import MongoClient
from pymongo.cursor import CursorType
import datetime
from datetime import date
import psutil
import time
from common.loggerConfig import setup_logger, setup_mongodb_monitor_logger
import atexit

log = setup_logger()
mongodb_monitor_log = setup_mongodb_monitor_logger()

class MongoDBHandler:
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBHandler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        importConf = importConfig()
        host = importConf.select_section("MONGODB")["host"]
        port = importConf.select_section("MONGODB")["port"]
        
        # 연결 옵션 추가
        self._client = MongoClient(
            host, 
            int(port),
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=45000,
            maxPoolSize=50,
            minPoolSize=10,
            maxIdleTimeMS=30000,
            waitQueueTimeoutMS=10000,
            retryWrites=True,
            retryReads=True
        )
        self._session = None
        self.monitor_connection()
        
        # 프로그램 종료 시 연결 정리
        atexit.register(self.cleanup)
        
        self._initialized = True

    def cleanup(self):
        """MongoDB 연결을 정리합니다."""
        try:
            if self._session:
                self._session.end_session()
                self._session = None
            
            if self._client:
                self._client.close()
                self._client = None
                
            mongodb_monitor_log.info("MongoDB 연결이 정상적으로 종료되었습니다.")
        except Exception as e:
            mongodb_monitor_log.error(f"MongoDB 연결 종료 중 오류 발생: {str(e)}")

    def monitor_connection(self):
        """MongoDB 연결 상태를 모니터링하고 로깅합니다."""
        try:
            # 서버 상태 확인
            server_status = self._client.admin.command('serverStatus')
            mongodb_monitor_log.info(f"MongoDB 서버 상태: {server_status['ok']}")
            
            # 연결 상태 확인
            connections = server_status.get('connections', {})
            mongodb_monitor_log.info(f"현재 연결 수: {connections.get('current', 0)}")
            mongodb_monitor_log.info(f"사용 가능한 연결 수: {connections.get('available', 0)}")
            
            # 메모리 사용량 확인
            memory = server_status.get('mem', {})
            mongodb_monitor_log.info(f"MongoDB 메모리 사용량: {memory.get('resident', 0)}MB")
            
            # 시스템 리소스 확인
            process = psutil.Process()
            system_memory = process.memory_info()
            mongodb_monitor_log.info(f"시스템 메모리 사용량: {system_memory.rss / 1024 / 1024:.2f}MB")
            
            # 연결 수가 너무 많으면 경고
            if connections.get('current', 0) > 100:
                mongodb_monitor_log.warning(f"연결 수가 많습니다: {connections.get('current', 0)}")
            
        except Exception as e:
            mongodb_monitor_log.error(f"MongoDB 모니터링 중 오류 발생: {str(e)}")

    def ensure_connection(self):
        """MongoDB 연결을 확인하고 필요시 재연결을 시도합니다."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self._client.admin.command('ping')
                mongodb_monitor_log.info("MongoDB 연결 정상")
                return True
            except pymongo.errors.ConnectionFailure as e:
                mongodb_monitor_log.warning(f"MongoDB 연결 실패 (시도 {retry_count + 1}/{max_retries}): {str(e)}")
                retry_count += 1
                time.sleep(5)
                
                if retry_count >= max_retries:
                    mongodb_monitor_log.error("MongoDB 연결 최대 재시도 횟수 초과")
                    return False
        return False

    def start_session(self):
        """세션을 시작하고 로깅합니다."""
        if self._session is None:
            try:
                self._session = self._client.start_session()
                mongodb_monitor_log.info("MongoDB 세션 시작")
            except Exception as e:
                mongodb_monitor_log.error(f"세션 시작 실패: {str(e)}")
                raise
        return self._session

    def end_session(self):
        """세션을 종료하고 로깅합니다."""
        if self._session is not None:
            try:
                self._session.end_session()
                mongodb_monitor_log.info("MongoDB 세션 종료")
            except Exception as e:
                mongodb_monitor_log.error(f"세션 종료 실패: {str(e)}")
            finally:
                self._session = None

    def ensure_unique_index(self, db_name, collection_name, field_name):
        self.validate_params(db_name, collection_name)
        current_indexes = self._client[db_name][collection_name].index_information()
        if field_name not in current_indexes:
            self._client[db_name][collection_name].create_index([(field_name, pymongo.ASCENDING)], unique=True)

    def validate_params(self, db_name, collection_name):
        if not db_name or not collection_name:
            raise Exception("Database name and collection name must be provided.")
    
    def insert_item(self, data, db_name=None, collection_name=None):
        """데이터 삽입 시 연결 상태를 확인하고 로깅합니다."""
        self.validate_params(db_name, collection_name)
        if not isinstance(data, dict):
            raise Exception("data type should be dict")
        
        try:
            if not self.ensure_connection():
                raise pymongo.errors.ConnectionFailure("MongoDB 연결 실패")
            
            result = self._client[db_name][collection_name].insert_one(data)
            mongodb_monitor_log.info(f"데이터 삽입 성공: {db_name}.{collection_name}")
            return result.inserted_id
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 삽입 실패: {str(e)}")
            raise

    def insert_items(self, datas, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if not isinstance(datas, list):
            raise Exception("datas type should be list")
        try:
            return self._client[db_name][collection_name].insert_many(datas).inserted_ids
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 일괄 삽입 실패: {str(e)}")
            raise

    def find_item(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        query_options = {"_id": False}
        if projection:
            query_options.update(projection)
        
        try:
            cursor = self._client[db_name][collection_name].find(condition, query_options)
            if sort:
                cursor.sort(sort)
            cursor.limit(1)
            return next(cursor, None)
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 조회 실패: {str(e)}")
            return None

    def find_items(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None, limit=None):
        """데이터 조회 시 연결 상태를 확인하고 로깅합니다."""
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        find_options = {}
        
        try:
            if not self.ensure_connection():
                raise pymongo.errors.ConnectionFailure("MongoDB 연결 실패")
            
            if projection:
                find_options['projection'] = projection
            
            cursor = self._client[db_name][collection_name].find(condition, **find_options)
            
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            
            result = list(cursor)
            mongodb_monitor_log.info(f"데이터 조회 성공: {db_name}.{collection_name} - {len(result)}개 문서")
            return result
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 조회 실패: {str(e)}")
            raise

    def find_items_distinct(self, db_name, collection_name, distinct_col, condition=None):
        """특정 컬럼의 고유값 목록을 반환"""
        try:
            with self._client.start_session() as session:
                with session.start_transaction():
                    collection = self._client[db_name][collection_name]
                    if condition:
                        return collection.distinct(distinct_col, condition, session=session)
                    return collection.distinct(distinct_col, session=session)
        except Exception as e:
            log.error(f"데이터 조회 중 오류 발생: {e}")
            return []

    def find_items_id(self, condition=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find(condition, {"_id": True}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST, session=session)

    def find_item_id(self, condition=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find_one(condition, {"_id": True}, session=session)

    def delete_items(self, condition=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict):
            raise Exception("Condition must be provided as dict")
        
        try:
            return self._client[db_name][collection_name].delete_many(condition)
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 삭제 실패: {str(e)}")
            raise

    def update_items(self, condition=None, update_value=None, db_name=None, collection_name=None):
        """데이터 업데이트 시 연결 상태를 확인하고 로깅합니다."""
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        
        try:
            if not self.ensure_connection():
                raise pymongo.errors.ConnectionFailure("MongoDB 연결 실패")
            
            result = self._client[db_name][collection_name].update_many(
                filter=condition, 
                update=update_value
            )
            mongodb_monitor_log.info(f"데이터 업데이트 성공: {db_name}.{collection_name} - {result.modified_count}개 문서 수정")
            return result
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 업데이트 실패: {str(e)}")
            raise

    def update_item(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        
        try:
            return self._client[db_name][collection_name].update_one(
                filter=condition, 
                update=update_value
            )
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 업데이트 실패: {str(e)}")
            raise
    
    def aggregate(self, pipeline=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if pipeline is None or not isinstance(pipeline, list):
            raise Exception("Pipeline must be provided as a list")
        
        try:
            return list(self._client[db_name][collection_name].aggregate(pipeline))
        except Exception as e:
            mongodb_monitor_log.error(f"집계 쿼리 실패: {str(e)}")
            raise

    def text_search(self, text=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if text is None or not isinstance(text, str):
            raise Exception("Text must be provided")
        return self._client[db_name][collection_name].find({"$text": {"$search": text}}, session=session)

    def upsert_item(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        
        try:
            return self._client[db_name][collection_name].update_one(
                filter=condition, 
                update=update_value, 
                upsert=True
            )
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 upsert 실패: {str(e)}")
            raise

    def upsert_items(self, condition=None, update_value=None, db_name=None, collection_name=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        
        try:
            return self._client[db_name][collection_name].update_many(
                filter=condition, 
                update=update_value, 
                upsert=True
            )
        except Exception as e:
            mongodb_monitor_log.error(f"데이터 일괄 upsert 실패: {str(e)}")
            raise

    def validate_params(self, db_name, collection_name=None):
        if not db_name:
            raise Exception("Database name must be provided.")
        if collection_name is not None and not collection_name:
            raise Exception("Collection name must be provided when specified.")
    
    def list_collections(self, db_name):
        """데이터베이스의 컬렉션 목록을 반환"""
        try:
            with self._client.start_session() as session:
                with session.start_transaction():
                    return self._client[db_name].list_collection_names(session=session)
        except Exception as e:
            log.error(f"컬렉션 목록 조회 중 오류 발생: {e}")
            return []
    
    def check_database_exists(self, db_name):
        self.validate_params(db_name)
        return len(self._client[db_name].list_collection_names()) > 0
    
    def delete_column(self, db_name, column_name):
        self.validate_params(db_name)
        collection_names = self.list_collections(db_name)
        for collection_name in collection_names:
            self._client[db_name][collection_name].update_many({}, {"$unset": {column_name: ""}})

    def monitor_status(self):
        # MongoDB 서버 상태, 연결 수, 메모리 사용량 등 모니터링
        try:
            # 서버 상태
            server_status = self._client.admin.command('serverStatus')
            mongodb_monitor_log.info(f"MongoDB 서버 상태: {server_status['ok']}")
            # 현재 연결 수
            connections = server_status.get('connections', {})
            mongodb_monitor_log.info(f"현재 연결 수: {connections.get('current', 'N/A')}")
            mongodb_monitor_log.info(f"사용 가능한 연결 수: {connections.get('available', 'N/A')}")
            # MongoDB 메모리 사용량
            mem = server_status.get('mem', {})
            mongodb_monitor_log.info(f"MongoDB 메모리 사용량: {mem.get('resident', 'N/A')}MB")
            # 시스템 메모리 사용량
            import psutil
            sys_mem = psutil.Process().memory_info().rss / 1024 / 1024
            mongodb_monitor_log.info(f"시스템 메모리 사용량: {sys_mem:.2f}MB")
        except Exception as e:
            mongodb_monitor_log.error(f"MongoDB 모니터링 중 오류 발생: {e}")

    def __del__(self):
        """소멸자에서도 연결을 정리합니다."""
        self.cleanup()
