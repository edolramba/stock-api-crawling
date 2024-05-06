import logging

def setup_logger():
    # 로거 생성
    logger = logging.getLogger('StatusBarLogger')
    logger.setLevel(logging.INFO)  # 로그 레벨 설정

    # 파일 핸들러 설정
    file_handler = logging.FileHandler('C:\\Dev\\stock-api-crawling\\log\\statusbar.log', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # 로거에 핸들러 추가
    logger.addHandler(file_handler)

    return logger
