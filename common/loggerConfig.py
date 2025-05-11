import logging
import os
from datetime import datetime, timedelta
import glob

def cleanup_old_logs(log_dir='logs', retention_days=14):
    """오래된 로그 파일을 삭제합니다."""
    try:
        # 현재 날짜
        current_date = datetime.now()
        # 보관 기간 이전 날짜
        cutoff_date = current_date - timedelta(days=retention_days)
        
        # 로그 파일 패턴
        log_patterns = [
            'mongodb_connection_*.log',
            'mongodb_monitor_*.log',
            'statusbar.log',
            'dataCrawler_*.log',
            'mongodb_command_*.log',
            'telegram_*.log',
            'import_config_*.log'
        ]
        
        # 각 패턴에 대해 오래된 파일 삭제
        for pattern in log_patterns:
            log_files = glob.glob(os.path.join(log_dir, pattern))
            for log_file in log_files:
                try:
                    # 파일의 생성 시간 확인
                    file_time = datetime.fromtimestamp(os.path.getctime(log_file))
                    if file_time < cutoff_date:
                        os.remove(log_file)
                        print(f"삭제된 오래된 로그 파일: {log_file}")
                except Exception as e:
                    print(f"로그 파일 삭제 중 오류 발생: {str(e)}")
    except Exception as e:
        print(f"로그 정리 중 오류 발생: {str(e)}")

def setup_logger():
    """기본 로거를 설정합니다."""
    # 로그 디렉토리 생성
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 현재 시간을 YYYY-MM-DD 형식으로 가져오기
    current_time = datetime.now().strftime('%Y-%m-%d')

    # 로거 설정
    logger = logging.getLogger('main')
    logger.setLevel(logging.DEBUG)

    # 기존 핸들러가 있다면 제거
    if logger.handlers:
        logger.handlers.clear()

    # 파일 핸들러 설정
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f'dataCrawler_{current_time}.log'),
        encoding='utf-8',
        mode='a'  # 추가 모드로 변경
    )
    file_handler.setLevel(logging.DEBUG)

    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # 포맷터 설정
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 로그 전파 방지
    logger.propagate = False

    return logger

def setup_mongodb_monitor_logger():
    """MongoDB 모니터링 로거를 설정합니다."""
    # 로그 디렉토리 생성
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 현재 시간을 YYYY-MM-DD 형식으로 가져오기
    current_time = datetime.now().strftime('%Y-%m-%d')

    # MongoDB 모니터링 로거 설정
    logger = logging.getLogger('mongodb_monitor')
    logger.setLevel(logging.DEBUG)

    # 기존 핸들러가 있다면 제거
    if logger.handlers:
        logger.handlers.clear()

    # 파일 핸들러 설정
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f'mongodb_monitor_{current_time}.log'),
        encoding='utf-8',
        mode='a'  # 추가 모드로 변경
    )
    file_handler.setLevel(logging.DEBUG)

    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # 포맷터 설정
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 로그 전파 방지
    logger.propagate = False

    return logger

def setup_telegram_logger():
    """Telegram 관련 로그를 위한 로거 설정"""
    logger = logging.getLogger('telegram')
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        logger.handlers.clear()
    
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    file_handler = logging.FileHandler(
        os.path.join(log_dir, 'telegram.log'),
        encoding='utf-8',
        mode='a'  # 추가 모드로 변경
    )
    file_handler.setLevel(logging.DEBUG)

    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    
    return logger

def setup_mongodb_command_logger():
    """MongoDB 명령 로거를 설정합니다."""
    # 로그 디렉토리 생성
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 현재 시간을 YYYY-MM-DD 형식으로 가져오기
    current_time = datetime.now().strftime('%Y-%m-%d')

    # MongoDB 명령 로거 설정
    logger = logging.getLogger('mongodb_command')
    logger.setLevel(logging.DEBUG)

    # 기존 핸들러가 있다면 제거
    if logger.handlers:
        logger.handlers.clear()

    # 파일 핸들러 설정
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f'mongodb_command_{current_time}.log'),
        encoding='utf-8',
        mode='a'  # 추가 모드로 변경
    )
    file_handler.setLevel(logging.DEBUG)

    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # 포맷터 설정
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 핸들러 추가
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 로그 전파 방지
    logger.propagate = False

    return logger

def setup_import_config_logger():
    """importConfig 관련 로그를 위한 로거 설정"""
    logger = logging.getLogger('import_config')
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        logger.handlers.clear()
    
    log_dir = 'log'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    file_handler = logging.FileHandler(
        os.path.join(log_dir, 'import_config.log'),
        encoding='utf-8',
        mode='a'  # 추가 모드로 변경
    )
    file_handler.setLevel(logging.DEBUG)

    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    
    return logger
