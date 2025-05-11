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
logger = logging.getLogger('minute_data_validation')
logger.setLevel(logging.INFO)

# 기존 핸들러가 있다면 제거
if logger.handlers:
    logger.handlers.clear()

# 파일 핸들러 설정
file_handler = logging.FileHandler(
    os.path.join(log_dir, 'minute_data_validation.log'),
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

class MinuteDataValidator:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.min_db = self.client['sp_1min']
        self.day_db = self.client['sp_day']
        self.bot = selfTelegram()
        self.missing_data_stocks = {}  # 데이터 누락 종목 저장
        
    def validate_minute_data(self, stock_code, date):
        """분봉 데이터의 유효성을 검증합니다."""
        try:
            # 해당 일자의 분봉 데이터 조회
            min_collection = self.min_db[stock_code]
            min_data = list(min_collection.find({'date': date}))
            
            if not min_data:
                logger.warning(f"{stock_code}: {date} 분봉 데이터 없음")
                if stock_code not in self.missing_data_stocks:
                    self.missing_data_stocks[stock_code] = set()
                self.missing_data_stocks[stock_code].add(date)
                return []
            
            # 해당 일자의 일봉 데이터 조회
            day_collection = self.day_db[stock_code]
            day_data = day_collection.find_one({'date': date})
            
            if not day_data:
                logger.warning(f"{stock_code}: {date} 일봉 데이터 없음")
                return []
            
            errors = []
            
            # 1. 분봉 데이터의 OHLC 논리 검증
            for minute in min_data:
                if not (minute['low'] <= minute['open'] <= minute['high'] and 
                       minute['low'] <= minute['close'] <= minute['high']):
                    errors.append({
                        'type': 'OHLC_LOGIC_ERROR',
                        'time': minute.get('time', 'unknown'),
                        'open': minute['open'],
                        'high': minute['high'],
                        'low': minute['low'],
                        'close': minute['close']
                    })
                    self.bot.send_message(
                        f"⚠️ {stock_code} {date} {minute.get('time', 'unknown')} OHLC 논리 오류\n"
                        f"Open: {minute['open']}, High: {minute['high']}, "
                        f"Low: {minute['low']}, Close: {minute['close']}"
                    )
            
            # 2. 분봉 데이터와 일봉 데이터 범위 비교
            min_high = max(m['high'] for m in min_data)
            min_low = min(m['low'] for m in min_data)
            
            if min_high > day_data['high'] * 1.001:  # 0.1% 오차 허용
                errors.append({
                    'type': 'HIGH_PRICE_MISMATCH',
                    'minute_high': min_high,
                    'daily_high': day_data['high']
                })
                self.bot.send_message(
                    f"⚠️ {stock_code} {date} 고가 불일치\n"
                    f"분봉 고가: {min_high}, 일봉 고가: {day_data['high']}"
                )
                
            if min_low < day_data['low'] * 0.999:  # 0.1% 오차 허용
                errors.append({
                    'type': 'LOW_PRICE_MISMATCH',
                    'minute_low': min_low,
                    'daily_low': day_data['low']
                })
                self.bot.send_message(
                    f"⚠️ {stock_code} {date} 저가 불일치\n"
                    f"분봉 저가: {min_low}, 일봉 저가: {day_data['low']}"
                )
            
            # 3. 거래량 검증
            if any(m['volume'] < 0 for m in min_data):
                errors.append({
                    'type': 'NEGATIVE_VOLUME',
                    'date': date
                })
                self.bot.send_message(f"⚠️ {stock_code} {date} 음수 거래량 발견")
            
            # 4. 시간 연속성 검증
            times = sorted([m['time'] for m in min_data])
            expected_times = self.generate_trading_times()
            missing_times = set(expected_times) - set(times)
            
            if missing_times:
                errors.append({
                    'type': 'MISSING_DATA',
                    'missing_times': list(missing_times)
                })
                self.bot.send_message(
                    f"⚠️ {stock_code} {date} 누락된 분봉 데이터\n"
                    f"누락 시간: {', '.join(sorted(missing_times))}"
                )
            
            return errors
            
        except Exception as e:
            logger.error(f"검증 실패 - {stock_code} {date}: {str(e)}")
            return []
    
    def generate_trading_times(self):
        """정상 거래 시간의 분봉 시간들을 생성합니다."""
        times = []
        for hour in range(9, 16):
            for minute in range(0, 60):
                if hour == 9 and minute < 1:  # 9시 1분부터
                    continue
                if hour == 15 and minute > 19:  # 15시 19분까지
                    continue
                times.append(f"{hour:02d}{minute:02d}")
        return times
    
    def validate_all_stocks(self):
        """모든 종목의 최근 분봉 데이터를 검증합니다."""
        end_date = int(datetime.now().strftime('%Y%m%d'))
        start_date = int((datetime.now() - timedelta(days=7)).strftime('%Y%m%d'))
        
        collections = self.min_db.list_collection_names()
        collections = [c for c in collections if c.startswith('A')]
        
        validation_results = {}
        for stock_code in collections:
            logger.info(f"검증 중: {stock_code}")
            
            # 각 일자별로 검증
            current_date = start_date
            while current_date <= end_date:
                errors = self.validate_minute_data(stock_code, current_date)
                
                if errors:
                    if stock_code not in validation_results:
                        validation_results[stock_code] = {}
                    validation_results[stock_code][current_date] = errors
                    logger.warning(f"{stock_code} {current_date} 오류 발견: {len(errors)}건")
                    
                    # 오류가 있는 경우 해당 일자의 분봉 데이터 삭제
                    self.delete_minute_data(stock_code, current_date)
                
                current_date = int((datetime.strptime(str(current_date), '%Y%m%d') + 
                                  timedelta(days=1)).strftime('%Y%m%d'))
        
        # 검증 결과 요약
        self.send_validation_summary(validation_results)
        return validation_results
    
    def send_validation_summary(self, validation_results):
        """검증 결과 요약을 전송합니다."""
        summary = "🔍 분봉 데이터 검증 결과 요약\n\n"
        
        # 데이터 누락 종목
        if self.missing_data_stocks:
            summary += f"⚠️ 데이터 누락 종목 ({len(self.missing_data_stocks)}개):\n"
            for stock_code, dates in self.missing_data_stocks.items():
                summary += f"{stock_code}: {len(dates)}일치 데이터 누락\n"
            summary += "\n"
        
        # 검증 오류 종목
        if validation_results:
            summary += f"❌ 검증 오류 종목 ({len(validation_results)}개):\n"
            for stock_code, dates in validation_results.items():
                summary += f"{stock_code}: {len(dates)}일치 오류\n"
        
        self.bot.send_message(summary)
    
    def delete_minute_data(self, stock_code, date):
        """특정 일자의 분봉 데이터를 삭제합니다."""
        try:
            collection = self.min_db[stock_code]
            result = collection.delete_many({'date': date})
            logger.info(f"{stock_code}: {date} 분봉 데이터 {result.deleted_count}건 삭제됨")
            self.bot.send_message(f"🗑️ {stock_code}: {date} 분봉 데이터 {result.deleted_count}건 삭제됨")
        except Exception as e:
            logger.error(f"데이터 삭제 실패 - {stock_code} {date}: {str(e)}")
    
    def save_validation_results(self, results):
        """검증 결과를 파일로 저장합니다."""
        try:
            with open(os.path.join(log_dir, 'minute_validation_results.txt'), 'w', encoding='utf-8') as f:
                # 데이터 누락 종목
                if self.missing_data_stocks:
                    f.write("\n=== 데이터 누락 종목 ===\n")
                    for stock_code, dates in self.missing_data_stocks.items():
                        f.write(f"\n{stock_code}:\n")
                        f.write(f"  누락 일자: {sorted(dates)}\n")
                
                # 검증 오류 종목
                if results:
                    f.write("\n=== 검증 오류 종목 ===\n")
                    for stock_code, dates in results.items():
                        f.write(f"\n{stock_code} 오류:\n")
                        for date, errors in dates.items():
                            f.write(f"  날짜: {date}\n")
                            for error in errors:
                                f.write(f"    유형: {error['type']}\n")
                                for key, value in error.items():
                                    if key != 'type':
                                        f.write(f"    {key}: {value}\n")
                                f.write("    ---\n")
            
            logger.info("검증 결과가 minute_validation_results.txt 파일에 저장되었습니다.")
        except Exception as e:
            logger.error(f"결과 저장 실패: {str(e)}")

if __name__ == "__main__":
    validator = MinuteDataValidator()
    results = validator.validate_all_stocks()
    validator.save_validation_results(results) 