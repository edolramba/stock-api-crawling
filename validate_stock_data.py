import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging
import os
from util.alarm.selfTelegram import selfTelegram

# 로그 디렉토리 생성
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 로깅 설정
logger = logging.getLogger('data_validation')
logger.setLevel(logging.INFO)

# 기존 핸들러가 있다면 제거
if logger.handlers:
    logger.handlers.clear()

# 파일 핸들러 설정
file_handler = logging.FileHandler(
    os.path.join(log_dir, 'data_validation.log'),
    encoding='utf-8',
    mode='a'
)
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 포맷터 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 로그 전파 방지
logger.propagate = False

class DataValidator:
    def __init__(self, db_name='sp_day'):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.error_threshold = 0.02  # 2% 오차 허용
        self.bot = selfTelegram()
        self.delisted_stocks = set()  # 상장폐지/거래중단 종목 저장
        self.missing_data_stocks = {}  # 데이터 누락 종목 저장

    def get_yahoo_data(self, stock_code, start_date, end_date, interval='1d'):
        """야후 파이낸스에서 주가 데이터를 가져옵니다."""
        try:
            # 종목코드 변환 (A123456 -> 123456.KS 또는 123456.KQ)
            code = stock_code[1:]  # 'A' 제거
            if stock_code in self.get_kospi_codes():
                yahoo_code = f"{code}.KS"
            else:
                yahoo_code = f"{code}.KQ"
            
            stock = yf.Ticker(yahoo_code)
            df = stock.history(interval=interval, start=start_date, end=end_date)
            
            # 상장폐지/거래중단 확인
            if df.empty:
                self.delisted_stocks.add(stock_code)
                logger.warning(f"{stock_code}: 상장폐지 또는 거래중단 종목으로 의심됨")
                self.bot.send_message(f"⚠️ {stock_code}: 상장폐지 또는 거래중단 종목으로 의심됨")
                return None
                
            return df
        except Exception as e:
            if "possibly delisted" in str(e).lower():
                self.delisted_stocks.add(stock_code)
                logger.warning(f"{stock_code}: 상장폐지 또는 거래중단 종목으로 확인됨")
                self.bot.send_message(f"⚠️ {stock_code}: 상장폐지 또는 거래중단 종목으로 확인됨")
            else:
                logger.error(f"야후 데이터 조회 실패 - {stock_code}: {str(e)}")
            return None

    def get_db_data(self, stock_code, start_date, end_date):
        """MongoDB에서 주가 데이터를 가져옵니다."""
        try:
            collection = self.db[stock_code]
            data = list(collection.find({
                'date': {
                    '$gte': int(start_date.strftime('%Y%m%d')),
                    '$lte': int(end_date.strftime('%Y%m%d'))
                }
            }))
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"DB 데이터 조회 실패 - {stock_code}: {str(e)}")
            return None

    def get_kospi_codes(self):
        """KOSPI 종목 코드 목록을 가져옵니다."""
        try:
            common_db = self.client['sp_common']
            kospi_codes = list(common_db['sp_all_code_name'].find(
                {'market_kind': 1},
                {'stock_code': 1, '_id': 0}
            ))
            return [code['stock_code'] for code in kospi_codes]
        except Exception as e:
            logger.error(f"KOSPI 종목 코드 조회 실패: {str(e)}")
            return []

    def check_missing_data(self, stock_code, db_data, yahoo_data):
        """데이터 누락 여부를 확인합니다."""
        if db_data is None or yahoo_data is None:
            return
            
        db_dates = set(db_data['date'].astype(str))
        yahoo_dates = set(yahoo_data.index.strftime('%Y%m%d'))
        
        # DB에 없는 데이터
        missing_in_db = yahoo_dates - db_dates
        if missing_in_db:
            if stock_code not in self.missing_data_stocks:
                self.missing_data_stocks[stock_code] = {'missing_in_db': set(), 'missing_in_yahoo': set()}
            self.missing_data_stocks[stock_code]['missing_in_db'].update(missing_in_db)
            
            # 3일 이상 누락된 경우 알림
            if len(missing_in_db) >= 3:
                self.bot.send_message(f"⚠️ {stock_code}: DB에 {len(missing_in_db)}일치 데이터 누락")
        
        # 야후에 없는 데이터
        missing_in_yahoo = db_dates - yahoo_dates
        if missing_in_yahoo:
            if stock_code not in self.missing_data_stocks:
                self.missing_data_stocks[stock_code] = {'missing_in_db': set(), 'missing_in_yahoo': set()}
            self.missing_data_stocks[stock_code]['missing_in_yahoo'].update(missing_in_yahoo)
            
            # 3일 이상 누락된 경우 알림
            if len(missing_in_yahoo) >= 3:
                self.bot.send_message(f"⚠️ {stock_code}: 야후에 {len(missing_in_yahoo)}일치 데이터 누락")

    def compare_data(self, stock_code, db_data, yahoo_data):
        """DB 데이터와 야후 데이터를 비교합니다."""
        if db_data is None or yahoo_data is None or db_data.empty or yahoo_data.empty:
            return None

        # 데이터 누락 확인
        self.check_missing_data(stock_code, db_data, yahoo_data)

        errors = []
        for _, db_row in db_data.iterrows():
            date_str = str(db_row['date'])
            date = datetime.strptime(date_str, '%Y%m%d')
            
            # 해당 날짜의 야후 데이터 찾기
            if date in yahoo_data.index:
                yahoo_row = yahoo_data.loc[date]
                
                # 가격 비교
                price_fields = {
                    'open': ('Open', db_row['open']),
                    'high': ('High', db_row['high']),
                    'low': ('Low', db_row['low']),
                    'close': ('Close', db_row['close'])
                }
                
                for field, (yahoo_field, db_value) in price_fields.items():
                    yahoo_value = yahoo_row[yahoo_field]
                    error_rate = abs(db_value - yahoo_value) / yahoo_value
                    
                    if error_rate > self.error_threshold:
                        errors.append({
                            'date': date_str,
                            'field': field,
                            'db_value': db_value,
                            'yahoo_value': yahoo_value,
                            'error_rate': error_rate
                        })
                        
                        # 오차가 큰 경우 알림
                        if error_rate > 0.05:  # 5% 이상 오차
                            self.bot.send_message(
                                f"⚠️ {stock_code} {date_str} {field} 오차 발견\n"
                                f"DB: {db_value:.2f}, 야후: {yahoo_value:.2f}, 오차율: {error_rate:.2%}"
                            )
        
        return errors

    def validate_all_stocks(self):
        """모든 종목의 데이터를 검증합니다."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # 전체 종목 코드 가져오기
        collections = self.db.list_collection_names()
        collections = [c for c in collections if c.startswith('A')]
        
        validation_results = {}
        for stock_code in collections:
            logger.info(f"검증 중: {stock_code}")
            
            # 데이터 가져오기
            db_data = self.get_db_data(stock_code, start_date, end_date)
            yahoo_data = self.get_yahoo_data(stock_code, start_date, end_date)
            
            # 데이터 비교
            errors = self.compare_data(stock_code, db_data, yahoo_data)
            
            if errors:
                validation_results[stock_code] = errors
                logger.warning(f"{stock_code} 오류 발견: {len(errors)}건")
                
                # 오류가 있는 경우 해당 날짜 이후 데이터 삭제
                earliest_error_date = min(int(error['date']) for error in errors)
                self.delete_data_after_date(stock_code, earliest_error_date)
        
        # 검증 결과 요약
        self.send_validation_summary(validation_results)
        return validation_results

    def send_validation_summary(self, validation_results):
        """검증 결과 요약을 전송합니다."""
        summary = "🔍 데이터 검증 결과 요약\n\n"
        
        # 상장폐지/거래중단 종목
        if self.delisted_stocks:
            summary += f"📉 상장폐지/거래중단 종목 ({len(self.delisted_stocks)}개):\n"
            summary += ", ".join(sorted(self.delisted_stocks)) + "\n\n"
        
        # 데이터 누락 종목
        if self.missing_data_stocks:
            summary += f"⚠️ 데이터 누락 종목 ({len(self.missing_data_stocks)}개):\n"
            for stock_code, missing in self.missing_data_stocks.items():
                if missing['missing_in_db']:
                    summary += f"{stock_code}: DB 누락 {len(missing['missing_in_db'])}일\n"
                if missing['missing_in_yahoo']:
                    summary += f"{stock_code}: 야후 누락 {len(missing['missing_in_yahoo'])}일\n"
            summary += "\n"
        
        # 가격 오차 종목
        if validation_results:
            summary += f"❌ 가격 오차 종목 ({len(validation_results)}개):\n"
            for stock_code, errors in validation_results.items():
                summary += f"{stock_code}: {len(errors)}건 오차\n"
        
        self.bot.send_message(summary)

    def delete_data_after_date(self, stock_code, date):
        """특정 날짜 이후의 데이터를 삭제합니다."""
        try:
            collection = self.db[stock_code]
            result = collection.delete_many({'date': {'$gte': date}})
            logger.info(f"{stock_code}: {date} 이후 {result.deleted_count}개 데이터 삭제됨")
            self.bot.send_message(f"🗑️ {stock_code}: {date} 이후 {result.deleted_count}개 데이터 삭제됨")
        except Exception as e:
            logger.error(f"데이터 삭제 실패 - {stock_code}: {str(e)}")

    def save_validation_results(self, results):
        """검증 결과를 파일로 저장합니다."""
        try:
            with open(os.path.join(log_dir, 'validation_results.txt'), 'w', encoding='utf-8') as f:
                # 상장폐지/거래중단 종목
                if self.delisted_stocks:
                    f.write("\n=== 상장폐지/거래중단 종목 ===\n")
                    for stock_code in sorted(self.delisted_stocks):
                        f.write(f"{stock_code}\n")
                
                # 데이터 누락 종목
                if self.missing_data_stocks:
                    f.write("\n=== 데이터 누락 종목 ===\n")
                    for stock_code, missing in self.missing_data_stocks.items():
                        f.write(f"\n{stock_code}:\n")
                        if missing['missing_in_db']:
                            f.write(f"  DB 누락: {sorted(missing['missing_in_db'])}\n")
                        if missing['missing_in_yahoo']:
                            f.write(f"  야후 누락: {sorted(missing['missing_in_yahoo'])}\n")
                
                # 가격 오차 종목
                if results:
                    f.write("\n=== 가격 오차 종목 ===\n")
                    for stock_code, errors in results.items():
                        f.write(f"\n{stock_code} 오류:\n")
                        for error in errors:
                            f.write(f"  날짜: {error['date']}\n")
                            f.write(f"  필드: {error['field']}\n")
                            f.write(f"  DB 값: {error['db_value']}\n")
                            f.write(f"  야후 값: {error['yahoo_value']}\n")
                            f.write(f"  오차율: {error['error_rate']:.2%}\n")
                            f.write("  ---\n")
            
            logger.info("검증 결과가 validation_results.txt 파일에 저장되었습니다.")
        except Exception as e:
            logger.error(f"결과 저장 실패: {str(e)}")

if __name__ == "__main__":
    # 일봉 데이터 검증
    day_validator = DataValidator('sp_day')
    day_results = day_validator.validate_all_stocks()
    day_validator.save_validation_results(day_results)
    
    # 분봉 데이터 검증
    min_validator = DataValidator('sp_1min')
    min_results = min_validator.validate_all_stocks()
    min_validator.save_validation_results(min_results) 