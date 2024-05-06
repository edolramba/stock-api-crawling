# coding=utf-8
import datetime
from common.loggerConfig import setup_logger

# 로거 설정
logger = setup_logger()

def call_printer(original_func):
    """original 함수 call 시, 현재 시간과 함수 명을 출력하는 데코레이터"""
    def wrapper(*args, **kwargs):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print('[{:.22s}] func `{}` is called'.format(timestamp, original_func.__name__))
        logger.info('[{:.22s}] func `{}` is called'.format(timestamp, original_func.__name__))
        return original_func(*args, **kwargs)

    return wrapper

def return_status_msg_setter(original_func):
    """
    original 함수 exit 후, log 에 표시할 문자열을 수정하는 데코레이터
    args[0]는 self 를 참조하게 된다
    """
    def wrapper(*args, **kwargs):
        ret = original_func(*args, **kwargs)

        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        msg = '`{}` 완료됨[{}]'.format(original_func.__name__, timestamp)
        print(msg)
        logger.info(msg)  # 로그 파일에 기록

        return ret

    return wrapper
