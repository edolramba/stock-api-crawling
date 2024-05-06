import sys
import gc

from api.creonAPI import CpStockChart, CpCodeMgr

from common.loggerConfig import setup_logger
from util.autoLogin import autoLogin
from util.MongoDBHandler import MongoDBHandler
from util import decorators
from util.pandas_to_pyqt_table import PandasModel
from util.utils import is_market_open, available_latest_date, preformat_cjk

from pymongo import UpdateOne

import pandas as pd
import tqdm
import time
from datetime import datetime, timedelta

log = setup_logger()  # 로거 설정

class MainWindow():
    def __init__(self):
        super().__init__()
        # AutoLogin 클래스를 사용하여 로그인
        self.autoLogin = autoLogin()
        
        # Creon API Import
        self.objStockChart = CpStockChart()
        self.objCodeMgr = CpCodeMgr()
        
        # Initialize MongoDBHandler
        self.db_handler = MongoDBHandler()

        self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버
        self.update_status_msg = ''  # log 에 출력할 메세지 저장 멤버
        self.return_status_msg = ''  # log 에 출력할 메세지 저장 멤버
        
        # 서버에 존재하는 종목코드 리스트와 로컬DB에 존재하는 종목코드 리스트
        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()
        self.sv_view_model = None
        self.db_view_model = None

        self.code_name_list_update()
        print("종목코드 및 종목명 업데이트 완료")
        
        # self.db_name = ['sp_day', 'sp_1min','sp_week', 'sp_month']
        self.db_name = ['sp_day', 'sp_1min']
        
        for db_name in self.db_name:
            self.db_name = db_name
            self.connect_code_list_view()
            self.update_price_db()
            if self.db_name == 'sp_day':
                self.update_marketC_col()

    def code_name_list_update(self):
        # 1. API 서버에서 종목코드와 종목명 가져오기
        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list, '종목명': sv_name_list})
        
        # 2. 현재 날짜를 YYYYMMDD 형식으로 포매팅
        today = int(datetime.today().strftime('%Y%m%d'))
        
        # 3. MongoDB에 데이터 upsert 하지만 오늘 날짜에 이미 업데이트된 항목은 제외
        for idx, row in self.sv_code_df.iterrows():
            condition = {'stock_code': row['종목코드'], 'date': today}
            existing_item = self.db_handler.find_item(condition, db_name='sp_common', collection_name='sp_all_code_name')
            if not existing_item:
                update_value = {
                    '$set': {
                        'date': today,
                        'stock_name': row['종목명'],
                        'stock_code': row['종목코드']
                    }
                }
                self.db_handler.upsert_item(condition={'stock_code': row['종목코드']}, update_value=update_value, db_name='sp_common', collection_name='sp_all_code_name')

    def connect_code_list_view(self):
        # 1. API 서버 종목코드 가져와서 종목명과 조합
        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list,'종목명': sv_name_list},columns=('종목코드', '종목명'))
        
        # 2. 로컬 DB 에 저장된 종목 정보를 가져와서 dataframe 으로 저장
        # 로컬 DB 에 저장된 종목 정보 가져와서 dataframe 으로 저장
        db_code_list = self.db_handler._client[self.db_name].list_collection_names()
        db_name_list = list(map(self.objCodeMgr.get_code_name, db_code_list))
        if len(db_name_list) == 0:
            log.info("%s 는 업데이트 된 종목 없음", self.db_name)
        else:
            log.info(db_name_list)
            for code in db_code_list:
                # 'date' 필드에 인덱스가 있는지 확인하고 없으면 생성
                # 인덱스 정보를 가져와서 'date' 인덱스가 있는지 확인
                indexes = self.db_handler._client[self.db_name][code].index_information()
                if 'date_1' not in indexes:  # 'date_1'는 오름차순 인덱스를 나타냄
                    # 'date' 필드에 대해 인덱스 생성
                    self.db_handler._client[self.db_name][code].create_index('date', name='date_1')
                    log.info("Index on 'date' created.")
                else:
                    log.info("Index on 'date' already exists.")
            
        # 3. 로컬 DB 에 저장된 종목의 Data 가 어느시점까지 저장되어 있는지 체크
        db_latest_list = []
        for db_code in db_code_list:
            latest_entry = self.db_handler.find_item({}, self.db_name, db_code, sort=[('date', -1)], projection={'date': 1})
            # min : 202204050901, day, week, month : 19860228
            db_latest_list.append(latest_entry['date'] if latest_entry else None)
            # print(db_code, "의 최근 저장된 날짜 : ", latest_entry)
            
        # 현재 db에 저장된 'date' column의 단위(분/일) 확인
        if db_latest_list:
            if self.db_name == 'sp_1min': # 1분봉인 경우
                print("======== 1min 수집중 입니다.========")
            elif self.db_name == 'sp_3min': # 3분봉인 경우
                print("======== 3min 입니다.======== ")
            elif self.db_name == 'sp_5min': # 5분봉인 경우
                print("======== 5min 입니다.======== ")
            elif self.db_name == 'sp_day': # 일봉인 경우    
                print("======== 일봉입니다.======== ")
            elif self.db_name == 'sp_week': # 주봉인 경우    
                print("======== week 입니다.======== ")
            elif self.db_name == 'sp_month': # 월봉인 경우
                print("======== month 입니다.======== ")
            elif self.db_name == None:
                print("======== 없는 DB 입니다.======== ")
            else: # 그 외 다른값인 경우
                print("분류가 없는 데이터 입니다.")

        self.db_code_df = pd.DataFrame(
                {'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                columns=('종목코드', '종목명', '갱신날짜'))

    # sp_ck DB 를 새로 만들고 여기에 컬렉션 생성 (sp_ck_1min, sp_ck_day, sp_ck_week, sp_ck_month), stockCode, lastUpdate 컬럼에 저장 (YYYYMMDDHHMM)
    def update_price_db(self):
        
        fetch_code_df = self.sv_code_df
        db_code_df = self.db_code_df
        
        if self.db_name == 'sp_1min': # 1분봉
            tick_unit = '분봉'
            count = 200000  # 서버 데이터 최대 reach 약 18.5만 이므로 (18/02/25 기준)
            tick_range = 1
            columns=['open', 'high', 'low', 'close', 'volume', 'value'] # 2024.05.05
        elif self.db_name == 'sp_3min': # 3분봉
            tick_unit = '분봉'
            count = 100000
            tick_range = 3
            columns=['open', 'high', 'low', 'close', 'volume', 'value'] # 2024.05.05
        elif self.db_name == 'sp_5min': # 5분봉
            tick_unit = '분봉'
            count = 100000
            tick_range = 5
            columns=['open', 'high', 'low', 'close', 'volume', 'value'] # 2024.05.05
        elif self.db_name == 'sp_day': # 일봉
            tick_unit = '일봉'
            count = 10000  # 10000개면 현재부터 1980년 까지의 데이터에 해당함. 충분.
            tick_range = 1
            columns=['open', 'high', 'low', 'close', 'volume', 'value', 'marketC'] # 2024.05.05
        elif self.db_name == 'sp_week': # 주봉
            tick_unit = '주봉'
            count = 2000
            columns=['open', 'high', 'low', 'close', 'volume', 'value'] # 2024.05.05
        elif self.db_name == 'sp_month': # 월봉
            tick_unit = '월봉'
            count = 500
            columns=['open', 'high', 'low', 'close', 'volume', 'value'] # 2024.05.05
        else: # 없음
            raise ValueError("Invalid database name provided")
        
        # 분봉/일봉에 대해서만 아래 코드가 효과가 있음.
        if not is_market_open():
            latest_date = available_latest_date()
            print("최초 latest_date : ", latest_date)
            if tick_unit == '일봉':
                latest_date = latest_date // 10000  # 나머지를 버리고 정수 부분만 반환
                log.info("일봉일 때 10000 으로 나누고 정수 반환값 : %s", latest_date)
            elif tick_unit == '월봉':
                latest_date = latest_date // 1000000  # 나머지를 버리고 정수 부분만 반환
                latest_date = latest_date * 100
                log.info("월봉일 때 10000 으로 나누고 곱하기 100 후 정수 반환값 : %s", latest_date)
            elif tick_unit == '주봉':
                latest_date = latest_date // 10000
                latest_date = int(self.get_weekly_date(latest_date))
                log.info("주봉일 때 연도와 주차 반환값 : %s", latest_date)
                
            # 이미 DB 데이터가 최신인 종목들은 가져올 목록에서 제외한다
            already_up_to_date_codes = db_code_df[db_code_df['갱신날짜'] == latest_date]['종목코드'].values
            log.info("이미 데이터가 최신인 종목들 : %s", already_up_to_date_codes)
            if already_up_to_date_codes.size > 0:
                # numpy의 isin 사용하여 비교
                fetch_code_df = fetch_code_df[~fetch_code_df['종목코드'].isin(already_up_to_date_codes)]
            else:
                # 모든 코드가 최신이 아니라면 fetch_code_df 변경 없음
                log.info("No codes are up to date.")

            print("데이터가 최신이 아닌 종목들")
            print(fetch_code_df)
            
        tqdm_range = tqdm.trange(len(fetch_code_df), ncols=100)
        # MongoDB에 데이터를 삽입하는 부분
        for i in tqdm_range:
            
            # start_time = time.time()
            
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code[0], code[1])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))

            from_date = 0
            if code[0] in self.db_code_df['종목코드'].tolist():
                latest_date_entry = self.db_handler.find_item({}, self.db_name, code[0], sort=[('date', -1)])
                # print("latest_date_entry : ", latest_date_entry)
                from_date = latest_date_entry['date'] if latest_date_entry else 0
                # print("from date : ", from_date)

            if tick_unit == '분봉':  # 분봉 데이터 받기
                if self.objStockChart.RequestMT(code[0], 'm', tick_range, count, self, from_date) == False:
                    continue
            elif tick_unit == '일봉':  # 일봉 데이터 받기
                if self.objStockChart.RequestDWM(code[0], 'D', count, self, from_date) == False:
                    continue
            elif tick_unit == '주봉':  #주봉 데이터 받기
                if self.objStockChart.RequestDWM(code[0], 'W', count, self, from_date) == False:
                    continue
            elif tick_unit == '월봉':  # 월봉 데이터 받기
                if self.objStockChart.RequestDWM(code[0], 'M', count, self, from_date) == False:
                    continue
            
            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            df = df.loc[:from_date].iloc[:-1] if from_date != 0 else df
            df = df.iloc[::-1]
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            # 'date' 열을 기준으로 중복된 데이터 제거
            df.drop_duplicates(subset='date', keep='last', inplace=True)

            # MongoDB에 데이터 삽입
            # 'date' 필드에 인덱스가 있는지 확인하고 없으면 생성
            # 인덱스 정보를 가져와서 'date' 인덱스가 있는지 확인
            indexes = self.db_handler._client[self.db_name][code['종목코드']].index_information()
            if 'date_1' not in indexes:  # 'date_1'는 오름차순 인덱스를 나타냄
                # 'date' 필드에 대해 인덱스 생성
                self.db_handler._client[self.db_name][code['종목코드']].create_index('date', name='date_1')
                log.info("Index on 'date' created.")
            # else:
                # print("Index on 'date' already exists.")
    
            operations = [
                UpdateOne({'date': rec['date']}, {'$set': rec}, upsert=True) 
                for rec in df.to_dict('records')]
            if operations:
                self.db_handler._client[self.db_name][code['종목코드']].bulk_write(operations, ordered=False)
                
            # end_time = time.time()
            # elapsed_time = end_time - start_time
            # print(f"{self.db_name} and {code['종목코드']} Function executed in {elapsed_time} seconds")
            # log.info((f"{self.db_name} and {code['종목코드']} writed in {elapsed_time} seconds"))
            
            del df
            gc.collect()

        self.update_status_msg = ''
        self.connect_code_list_view()
        
    def get_weekly_date(self, latest_date):
        latest_date_str = str(latest_date)
        date_object = datetime.strptime(latest_date_str, '%Y%m%d')
        
        # 해당 월의 첫 날
        first_day_of_month = datetime(date_object.year, date_object.month, 1)
        
        # 해당 월의 첫 번째 일요일 찾기
        first_sunday = first_day_of_month + timedelta(days=(6 - first_day_of_month.weekday()))
        
        # 주차 계산 (첫 일요일 이전 날짜들은 첫 주차로 계산)
        if date_object < first_sunday:
            week_of_month = 1
        else:
            week_of_month = ((date_object - first_sunday).days // 7) + 1
        
        # 날짜 형식 YYYYMMWW (WW는 주차 * 10)
        formatted_date = f"{date_object.year}{date_object.month:02}{week_of_month * 10}"
        
        return formatted_date

    # sp_day 에 marketC 컬럼을 추가
    def update_marketC_col(self):
        
        fetch_code_df = self.sv_code_df # API 서버에 있는 종목코드, 종목명 리스트
        db_code_df = self.db_code_df # DB 에 있는 종목코드, 종목명 리스트
        
        tick_unit = '일봉'
        count = 10000  # 10000개면 현재부터 1980년 까지의 데이터에 해당함. 충분.
        tick_range = 1
        columns=['marketC'] # 2024.05.05
        
        if not is_market_open(): # 장 중이 아니라면
            latest_date = available_latest_date() # 최근 영업일 202405051030 형식
            if tick_unit == '일봉':
                latest_date = latest_date // 10000  # 연,월,일 남기고 시,분 제거
                
            # 이미 DB 데이터가 최신인 종목들은 가져올 목록에서 제외한다
            
            # 로컬 DB 에 저장된 종목의 marketC 컬럼 Data 가 어느시점까지 저장되어 있는지 체크
            # 없으면 처음부터 받고, 있으면 가장 최근까지 채워진 날짜를 검사해서 그 이후로 이어서 받는다.
            db_marketC_latest_dates = []
            # 각 종목 코드별로 MongoDB에서 marketC 컬럼의 최신 날짜를 확인
            for db_code in db_code_df['종목코드'].tolist():
                latest_entry = self.db_handler.find_item(
                    {'marketC': {'$exists': True}},  # marketC 컬럼이 있는 문서만 대상
                    self.db_name,
                    db_code,
                    sort=[('date', -1)],
                    projection={'date': 1}
                )
                if latest_entry:
                    db_marketC_latest_dates.append(latest_entry['date'])
                    # print("latest_entry : ", latest_entry)
                else:
                    db_marketC_latest_dates.append(None)  # marketC 데이터가 없는 경우 None을 추가

            # 최신 데이터가 있는 종목 코드 확인
            already_up_to_date_codes = [
                code for code, date in zip(db_code_df['종목코드'].tolist(), db_marketC_latest_dates)
                if date == latest_date
            ]
            # already_up_to_date_codes = db_code_df[db_code_df['갱신날짜'] == latest_date]['종목코드'].values
            log.info("marketC 컬럼이 이미 최신인 종목들 : %s", already_up_to_date_codes)
            if len(already_up_to_date_codes) > 0:
                # numpy의 isin 사용하여 비교
                fetch_code_df = fetch_code_df[~fetch_code_df['종목코드'].isin(already_up_to_date_codes)]
            else:
                # 모든 코드가 최신이 아니라면 fetch_code_df 변경 없음
                log.info("No codes are up to date.")

            print("데이터가 최신이 아닌 종목들")
            print("fetch_code_df : ", fetch_code_df)
            
        tqdm_range = tqdm.trange(len(fetch_code_df), ncols=100)
        # MongoDB에 데이터를 삽입하는 부분
        for i in tqdm_range:
            
            # start_time = time.time()
            
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code[0], code[1])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))

            from_date = 0
            if code[0] in self.db_code_df['종목코드'].tolist():
                # marketC 컬럼이 있는 문서 중 가장 최신의 날짜를 찾음
                latest_date_entry = self.db_handler.find_item(
                    {'marketC': {'$exists': True}}, 
                    self.db_name, 
                    code[0], 
                    sort=[('date', -1)],
                    projection={'date': 1}
                )
                from_date = latest_date_entry['date'] if latest_date_entry else 0
            if tick_unit == '일봉':  # 일봉 데이터 받기
                if self.objStockChart.RequestDWM(code[0], 'D', count, self, from_date) == False:
                    continue
            
            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            df = df.loc[:from_date].iloc[:-1] if from_date != 0 else df
            df = df.iloc[::-1]
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            # 'date' 열을 기준으로 중복된 데이터 제거
            df.drop_duplicates(subset='date', keep='last', inplace=True)

            # MongoDB에 데이터 삽입
            operations = [
                UpdateOne({'date': rec['date']}, {'$set': {'marketC': rec['marketC']}}, upsert=False)
                for rec in df[['date', 'marketC']].to_dict('records') if 'marketC' in rec
            ]
            if operations:
                self.db_handler._client[self.db_name][code['종목코드']].bulk_write(operations, ordered=False)

            del df
            gc.collect()

        self.update_status_msg = ''
        self.connect_code_list_view()

if __name__ == "__main__":
    MainWindow()