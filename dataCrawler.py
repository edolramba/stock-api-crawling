import sys
import gc
import asyncio

from api.creonAPI import CpStockChart, CpCodeMgr, CpStockUniWeek

from common.loggerConfig import setup_logger
from util.autoLogin import autoLogin
from util.MongoDBHandler import MongoDBHandler
from util.utils import is_market_open, available_latest_date, preformat_cjk

from pymongo import UpdateOne

import pandas as pd
import tqdm
# import time as t  # Remove this import
from time import sleep  # Use specific functions from the time module
from datetime import datetime, time as dt_time, timedelta

log = setup_logger()  # 로거 설정

class MainWindow():
    def __init__(self):
        super().__init__()
        # AutoLogin 클래스를 사용하여 로그인
        self.autoLogin = autoLogin()
        
        # Creon API Import
        self.objStockChart = CpStockChart()
        self.objCodeMgr = CpCodeMgr()
        self.objStockUniWeek = CpStockUniWeek()
        
        # Initialize MongoDBHandler
        self.db_handler = MongoDBHandler()

        self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버
        self.rcv_data2 = dict()
        self.update_status_msg = ''  # log 에 출력할 메세지 저장 멤버
        self.return_status_msg = ''  # log 에 출력할 메세지 저장 멤버
        
        # 서버에 존재하는 종목코드 리스트와 로컬DB에 존재하는 종목코드 리스트
        self.sv_code_df = pd.DataFrame()
        self.db_code_df = pd.DataFrame()
        self.sv_view_model = None
        self.db_view_model = None
        
        # self.delete_outTime_column() # 실수했을 때 해당일 날짜 지우기
        self.semaphore = asyncio.Semaphore(3) # 동시에 실행할 수 있는 최대 호출 수 설정
        self.loop = asyncio.get_event_loop()
        
        self.code_name_list_update()
        print("종목코드 및 종목명 업데이트 완료")  # self.sv_code_df
        # self.db_name = ['sp_day', 'sp_1min','sp_week', 'sp_month']
        # self.db_name = ['sp_day','sp_1min']
        self.db_name = ['sp_1min', 'sp_day']

        for db_name in self.db_name:
            self.db_name = db_name
            self.connect_code_list_view()
            self.loop.run_until_complete(self.update_price_db())
            if self.db_name == 'sp_day':
                print("======== 시간외 단일가 수집 중 입니다. ========")
                self.loop.run_until_complete(self.schedule_outTime())
                print("======== 시간외 단일가 수집완료 ========")
                        
    def code_name_list_update(self):
        # 1. API 서버에서 종목코드와 종목명 가져오기
        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        sv_market_list = list(map(self.objCodeMgr.get_market_kind, sv_code_list))
        sv_status_list  = list(map(self.objCodeMgr.get_code_status, sv_code_list))
        # 소속부 0:구분없음, 1:거래소, 2:코스닥, 3:K-OTC, 4:KRM, 5:KONEX  종목상태 : 0:정상, 1:거래정지, 2:거래중단
        self.sv_code_df = pd.DataFrame({'종목코드': sv_code_list, '종목명': sv_name_list, '소속부': sv_market_list, '종목상태': sv_status_list})
        
        # KOSPI와 KOSDAQ 업종코드 추가
        additional_data = pd.DataFrame({
            '종목코드': ['U001', 'U201'],
            '종목명': ['KOSPI', 'KOSDAQ'],
            '소속부': [1, 2],
            '종목상태': 0
        })
        
        # 기존 DataFrame에 새로운 데이터 추가
        self.sv_code_df = pd.concat([self.sv_code_df, additional_data], ignore_index=True)

        if not is_market_open():
            latest_date = available_latest_date()
            latest_date = latest_date // 10000  # 나머지를 버리고 정수 부분만 반환
            # 2. 현재 날짜를 YYYYMMDD 형식으로 포매팅
            # today = int(datetime.today().strftime('%Y%m%d'))
        
        # 3. MongoDB에 데이터 upsert 하지만 오늘 날짜에 이미 업데이트된 항목은 제외
        for idx, row in self.sv_code_df.iterrows():
            condition = {'stock_code': row['종목코드']}
            update_value = {
                '$set': {
                    'date': latest_date,
                    'stock_name': row['종목명'],
                    'stock_code': row['종목코드'],
                    'market_kind': row['소속부'],
                    'stock_status': row['종목상태']
                },
                '$setOnInsert': {
                    'sp_1min': None,
                    'sp_day': None,
                    'sp_week': None,
                    'sp_month': None
                }
            }
            self.db_handler.upsert_item(condition, update_value, db_name='sp_common', collection_name='sp_all_code_name')
        
        # 4. Local MongoDB의 sp_day DB에서 종목코드 컬렉션 목록 가져오기
        local_code_list = self.db_handler.list_collections('sp_day')
        # 5. sp_all_code_name 컬렉션에서 현재 존재하는 종목코드 목록 가져오기
        existing_codes = self.db_handler.find_items_distinct(db_name='sp_common', collection_name='sp_all_code_name', distinct_col='stock_code')
        # 6. sp_all_code_name에 없는 종목코드 추가
        for code in local_code_list:
            if code not in existing_codes:
                condition = {'stock_code': code}
                update_value = {
                    '$set': {
                        'date': latest_date,
                        'stock_name': '없음',
                        'stock_code': code,
                        'market_kind': 0,
                        'stock_status': 2
                    },
                    '$setOnInsert': {
                        'sp_1min': None,
                        'sp_day': None,
                        'sp_week': None,
                        'sp_month': None
                    }
                }
                self.db_handler.upsert_item(condition, update_value, db_name='sp_common', collection_name='sp_all_code_name')

    def connect_code_list_view(self):
        db_code_list = self.db_handler._client[self.db_name].list_collection_names()
        db_name_list = list(map(self.objCodeMgr.get_code_name, db_code_list))
        if len(db_name_list) == 0:
            log.info("%s 는 업데이트 된 종목 없음", self.db_name)
        else:
            log.info(db_name_list)
            for code in db_code_list:
                indexes = self.db_handler._client[self.db_name][code].index_information()
                if 'date_1' not in indexes:
                    self.db_handler._client[self.db_name][code].create_index('date', name='date_1')
                    log.info("Index on 'date' created.")
                else:
                    log.info("Index on 'date' already exists.")
        
        db_latest_list = []
        for db_code in db_code_list:
            latest_entry = self.db_handler.find_item({}, self.db_name, db_code, sort=[('date', -1)], projection={'date': 1})
            db_latest_list.append(latest_entry['date'] if latest_entry else None)
        
        if db_latest_list:
            if self.db_name == 'sp_1min':
                print("======== 1min 수집중 입니다.========")
            elif self.db_name == 'sp_day':
                print("======== 일봉 수집중 입니다.======== ")
            elif self.db_name == 'sp_week':
                print("======== 주봉 수집중 입니다.======== ")
            elif self.db_name == 'sp_month':
                print("======== 월봉 수집중 입니다.======== ")
            else:
                print("분류가 없는 데이터 입니다.")
        
        self.db_code_df = pd.DataFrame(
                {'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                columns=('종목코드', '종목명', '갱신날짜'))

        # sp_common DB에서 latest_date와 동일한 self.db_name 컬럼의 데이터를 가져와서 self.db_code_df에 추가
        latest_date = available_latest_date()
        if not is_market_open():
            if self.db_name == 'sp_1min':
                latest_date = latest_date // 10000
            additional_codes = self.db_handler.find_items(
                {self.db_name: latest_date},
                db_name='sp_common',
                collection_name='sp_all_code_name',
                projection={'stock_code': 1, 'stock_name': 1, self.db_name: 1, '_id': 0}
            )
            
            if additional_codes:
                additional_codes_df = pd.DataFrame(additional_codes)
                additional_codes_df.rename(columns={self.db_name: '갱신날짜', 'stock_code': '종목코드', 'stock_name': '종목명'}, inplace=True)
                
                # sp_1min의 경우, date 값을 1530으로 변환
                if self.db_name == 'sp_1min':
                    additional_codes_df['갱신날짜'] = additional_codes_df['갱신날짜'].apply(lambda x: int(str(x) + '1530'))

                # 기존 데이터프레임에 추가 데이터프레임을 병합하고 중복 종목 코드를 제거하여 최신 값으로 대체
                self.db_code_df = pd.concat([self.db_code_df, additional_codes_df], ignore_index=True).drop_duplicates(subset='종목코드', keep='last')

        print(f"당일 업데이트 완료된 데이터 및 건수: {len(self.db_code_df)}")
        print(self.db_code_df)
    
    async def update_price_db(self):
        fetch_code_df = self.sv_code_df[self.sv_code_df['종목상태'] == 0]
        db_code_df = self.db_code_df
        
        if self.db_name == 'sp_1min':
            tick_unit = '분봉'
            count = 2000
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value']
        elif self.db_name == 'sp_day':
            tick_unit = '일봉'
            count = 14
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value', 'marketC']
        elif self.db_name == 'sp_week':
            tick_unit = '주봉'
            count = 2000
            columns = ['open', 'high', 'low', 'close', 'volume', 'value']
        elif self.db_name == 'sp_month':
            tick_unit = '월봉'
            count = 500
            columns = ['open', 'high', 'low', 'close', 'volume', 'value']
        else:
            raise ValueError("Invalid database name provided")

        if not is_market_open():
            latest_date = available_latest_date()
            if tick_unit == '일봉':
                latest_date = latest_date // 10000
                log.info("일봉일 때 10000 으로 나누고 정수 반환값 : %s", latest_date)
            elif tick_unit == '월봉':
                latest_date = latest_date // 1000000
                latest_date = latest_date * 100
                log.info("월봉일 때 10000 으로 나누고 곱하기 100 후 정수 반환값 : %s", latest_date)
            elif tick_unit == '주봉':
                latest_date = latest_date // 10000
                latest_date = int(self.get_weekly_date(latest_date))
                log.info("주봉일 때 연도와 주차 반환값 : %s", latest_date)
                    
            already_up_to_date_codes = db_code_df[db_code_df['갱신날짜'] == latest_date]['종목코드'].values
                
            log.info("이미 데이터가 최신인 종목들 : %s", already_up_to_date_codes)
            if already_up_to_date_codes.size > 0:
                fetch_code_df = fetch_code_df[~fetch_code_df['종목코드'].isin(already_up_to_date_codes)]
            else:
                log.info("No codes are up to date.")

        if fetch_code_df.empty:
            print("없음")
        else:
            print(fetch_code_df)
        
        # 수집 완료 flag 에 기록할 latest_date 변수를 최근 날짜로 고정시키는 작업
        if self.db_name == 'sp_1min':
            latest_date = latest_date // 10000
            print("updated latest_date : ", latest_date)
        
        tqdm_range = tqdm.tqdm(total=len(fetch_code_df), ncols=100)
        
        tasks = []
        for i in range(len(fetch_code_df)):
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code[0], code[1])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))
            tasks.append(self.update_price_for_code(code, tick_unit, count, columns, tick_range, latest_date, tqdm_range))
            
        await asyncio.gather(*tasks)
        
        tqdm_range.close()
        
        if self.db_name == 'sp_1min':
            print(f"======== {self.db_name} 가격 데이터 수집 완료 ========")
        elif self.db_name == 'sp_day':
            print(f"======== {self.db_name} 가격 데이터 수집 완료 ========")

    async def update_price_for_code(self, code, tick_unit, count, columns, tick_range, latest_date, tqdm_range):
        async with self.semaphore:
            await self.objStockChart.apply_delay()
            from_date = 0
            if code[0] in self.db_code_df['종목코드'].tolist():
                latest_date_entry = self.db_handler.find_item({}, self.db_name, code[0], sort=[('date', -1)])
                from_date = latest_date_entry['date'] if latest_date_entry else 0

            if tick_unit == '분봉':
                success = await self.objStockChart.RequestMT(code[0], 'm', tick_range, count, self, from_date)
            elif tick_unit == '일봉':
                success = await self.objStockChart.RequestDWM(code[0], 'D', count, self, from_date)
            elif tick_unit == '주봉':
                success = await self.objStockChart.RequestDWM(code[0], 'W', count, self, from_date)
            elif tick_unit == '월봉':
                success = await self.objStockChart.RequestDWM(code[0], 'M', count, self, from_date)

            if not success:
                return
             
            if 'date' not in self.rcv_data:
                log.error(f"'date' key not found in rcv_data for code {code[0]}")
                return
            
            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            df = df.loc[:from_date].iloc[:-1] if from_date != 0 else df
            df = df.iloc[::-1]
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            df.drop_duplicates(subset='date', keep='last', inplace=True)

            indexes = self.db_handler._client[self.db_name][code['종목코드']].index_information()
            if 'date_1' not in indexes:
                self.db_handler._client[self.db_name][code['종목코드']].create_index('date', name='date_1')
                log.info("Index on 'date' created.")

            operations = [
                UpdateOne({'date': rec['date']}, {'$set': rec}, upsert=True) 
                for rec in df.to_dict('records')]
            if operations:
                self.db_handler._client[self.db_name][code['종목코드']].bulk_write(operations, ordered=False)

            del df
            gc.collect()
            
            # 수집이 완료되면 sp_all_code_name 에 각 DB 의 컬렉션명으로 수집완료 처리
            self.db_handler.update_item(
                {'stock_code': code[0]},
                {'$set': {self.db_name: latest_date}},
                db_name='sp_common',
                collection_name='sp_all_code_name'
            )
            tqdm_range.update(1)  # 한 종목 코드 완료 시 프로그레스바 업데이트

    async def schedule_outTime(self):
        # 현재 시간을 확인
        current_time = datetime.now()
        target_time = datetime.combine(current_time.date(), dt_time(18, 1))
        today_weekday = current_time.weekday()

        # 주말이면 바로 실행
        if today_weekday >= 5:  # Saturday or Sunday
            await self.handle_outTime()
        else:
            # 현재 시간이 오후 6시 1분 이후인지 확인
            if current_time.time() >= dt_time(18, 1):
                await self.handle_outTime()  # 시간외 단일가 데이터 업데이트 함수 추가
            else:
                print(f"현재 시간은 {current_time.strftime('%H:%M:%S')}입니다. 오후 6시 1분까지 기다립니다.")
                # 남은 시간 계산
                time_to_wait = (target_time - current_time).total_seconds()

                # 대기 시작 알림
                print(f"기다리는 중입니다. 남은 시간: {int(time_to_wait // 60)}분 {int(time_to_wait % 60)}초")

                while time_to_wait > 0:
                    # 5분마다 남은 시간을 출력
                    if time_to_wait % 300 == 0 or time_to_wait < 300:
                        remaining_minutes = int(time_to_wait // 60)
                        remaining_seconds = int(time_to_wait % 60)
                        print(f"남은 시간: {remaining_minutes}분 {remaining_seconds}초")

                    # 1초 대기
                    sleep(1)
                    time_to_wait -= 1

                await self.handle_outTime()  # 시간외 단일가 데이터 업데이트 함수 추가

    # sp_day 에 marketC 컬럼을 추가
    async def update_marketC_col(self):
        
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
            
        tqdm_range = tqdm.tqdm(total=len(fetch_code_df), ncols=100)
        # MongoDB에 데이터를 삽입하는 부분
        tasks = []
        for i in range(len(fetch_code_df)):
            await self.objStockChart.apply_delay()
            # start_time = time.time()
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code[0], code[1])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))
            tasks.append(self.update_marketC_for_code(code, tick_unit, count, columns, tick_range, tqdm_range))

        await asyncio.gather(*tasks)
        tqdm_range.close()

    async def update_marketC_for_code(self, code, tick_unit, count, columns, tick_range, tqdm_range):
        async with self.semaphore:
            await self.objStockChart.apply_delay()
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
                success = await self.objStockChart.RequestDWM(code[0], 'D', count, self, from_date)
                if not success:
                    return
            
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
            tqdm_range.update(1)  # 한 종목 코드 완료 시 프로그레스바 업데이트

    async def handle_outTime(self):

        all_collections = self.db_handler._client['sp_day'].list_collection_names()
        
        # 제외할 collection 이름들
        exclude_collections = {'U001', 'U201'}
        
        # sp_common DB의 sp_all_code_name 컬렉션에서 stock_code가 sp_day의 컬렉션명인 데이터 조회
        for collection in all_collections:
            stock_info = self.db_handler.find_item(
                {'stock_code': collection}, 
                db_name='sp_common', 
                collection_name='sp_all_code_name', 
                projection={'stock_status': 1, '_id': 0}
            )
            if stock_info and stock_info.get('stock_status', 0) != 0:
                exclude_collections.add(collection)
        
        # 제외한 collection 이름 목록을 생성합니다.
        collections = [col for col in all_collections if col not in exclude_collections]
        
        outTimeData = []
        for code in collections:
            if not is_market_open(): # 장 중이 아니라면
                price_latest = self.db_handler.find_item({}, 'sp_day', code, sort=[('date', -1)])
                price_lastest_date = price_latest['date']
                
                if self.db_handler.find_item({'diff_rate': {'$exists': True}}, 'sp_day', code, sort=[('date', -1)]):
                    latest_entry_with_diff_rate = self.db_handler.find_item({'diff_rate': {'$exists': True}}, 'sp_day', code, sort=[('date', -1)])
                    if latest_entry_with_diff_rate['date'] >= price_lastest_date:
                        pass
                    else:
                        condition = {'stock_code': code}
                        stock_name = self.db_handler.find_item(
                            condition, 
                            db_name='sp_common', 
                            collection_name='sp_all_code_name',
                            projection={'stock_name': 1, '_id': 0}
                        )
                        outTimeData.append({'종목코드': code, '종목명': stock_name})
                else:
                    condition = {'stock_code': code}
                    item = self.db_handler.find_item(condition, db_name='sp_common', collection_name='sp_all_code_name')
                    stock_name = item['stock_name'] if item else None
                    outTimeData.append({'종목코드': code, '종목명': stock_name})

        fetch_code_df = pd.DataFrame(outTimeData)
        print("fetch_code_df : ", len(fetch_code_df))

        count = 200
        tqdm_range = tqdm.tqdm(total=len(fetch_code_df), ncols=100)
        
        tasks = []
        for i in range(len(fetch_code_df)):
            code = fetch_code_df.iloc[i]
            self.return_status_msg = '[{}] {}'.format(code['종목코드'], code['종목명'])
            tqdm_range.set_description(self.return_status_msg)
            tasks.append(self.update_outTime_for_code(code, count, tqdm_range))
            
        await asyncio.gather(*tasks)
        tqdm_range.close()

        
    async def update_outTime_for_code(self, code, count, tqdm_range):
        async with self.semaphore:
            await self.objStockUniWeek.apply_delay()
            from_date = 0
            # diff_rate가 있는 가장 최신의 date를 찾음
            latest_entry_with_diff_rate = self.db_handler.find_item({'diff_rate': {'$exists': True}}, 'sp_day', code['종목코드'], sort=[('date', -1)])
            # 해당 종목코드의 데이터 중 가장 오래된 날짜를 찾음
            earliest_entry = self.db_handler.find_item({}, 'sp_day', code['종목코드'], sort=[('date', 1)])
            
            # 데이터가 존재하지 않으면 diff_rate를 요청하지 않음
            if not earliest_entry:
                return

            if latest_entry_with_diff_rate:
                from_date = latest_entry_with_diff_rate['date']
            else:
                from_date = earliest_entry['date']

            success = await self.objStockUniWeek.request_stock_data(code['종목코드'], count, self, from_date)

            if not success:
                return

            df = pd.DataFrame(self.rcv_data2)
            if 'date' in df.columns:
                df = df[df['date'] > from_date].iloc[::-1]
                df.reset_index(inplace=True, drop=True)

                df.drop_duplicates(subset='date', keep='last', inplace=True)
                df.dropna(subset=['date'], inplace=True)  # date 컬럼이 null인 행 제거
                operations = [
                    UpdateOne({'date': rec['date']}, {'$set': {'diff_rate': rec['diff_rate']}}, upsert=False)
                    for rec in df.to_dict('records')
                ]
                if operations:
                    self.db_handler._client['sp_day'][code['종목코드']].bulk_write(operations, ordered=False)
            
            del df
            gc.collect()
            tqdm_range.update(1)  # 한 종목 코드 완료 시 프로그레스바 업데이트
        
    # # sp_day 의 특정 날짜의 수집 데이터 삭제하기
    # def delete_outTime_column(self):
    #     # sp_day 의 특정 날짜의 수집 데이터 삭제하기
    #     condition = {"date": 20240802}
    #     collections = self.db_handler.list_collections("sp_day")
    #     for collection in collections:
    #         result = self.db_handler.delete_items(condition, db_name="sp_day", collection_name=collection)
    #         print(f"Deleted {result.deleted_count} documents from collection {collection}")

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

if __name__ == "__main__":
    MainWindow()
