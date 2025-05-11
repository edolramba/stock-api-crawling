import sys
import os
import io
import gc
import asyncio
import pandas as pd
import tqdm
from datetime import datetime, timedelta, time as dt_time
from time import sleep

from api.creonAPI import CpStockChart, CpCodeMgr, CpStockUniWeek
from common.loggerConfig import setup_logger
from util.autoLogin import autoLogin
from util.MongoDBHandler import MongoDBHandler
from util.utils import is_market_open, available_latest_date, preformat_cjk
from pymongo import UpdateOne
from util.alarm.selfTelegram import selfTelegram

import traceback  # 예외 로그 기록용
import pythoncom  # 추가 임포트 필요

# 표준 출력 및 에러 출력 UTF-8로 설정
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

log = setup_logger()  # 로거 설정

class MainWindow():
    def __init__(self):
        try:
            log.info("MainWindow 초기화 시작")

            super().__init__()

            # AutoLogin 객체 초기화 및 로그인
            log.info("AutoLogin 초기화 시작")
            self.autoLogin = autoLogin()
            log.info("AutoLogin 초기화 완료")

           # Telegram Bot 초기화
            log.info("Telegram Bot 초기화 시작")
            self.bot = selfTelegram()
            log.info("Telegram Bot 초기화 완료")
            
            # Creon API Import
            log.info("Creon API Import 시작")
            self.objStockChart = CpStockChart()
            self.objCodeMgr = CpCodeMgr()
            self.objStockUniWeek = CpStockUniWeek()
            log.info("Creon API Import 완료")
            
            # MongoDBHandler 초기화
            log.info("MongoDBHandler 초기화 시작")
            self.db_handler = MongoDBHandler()
            log.info("MongoDBHandler 초기화 완료")

            self.rcv_data = dict()  # RQ후 받아온 데이터 저장 멤버
            self.rcv_data2 = dict()
            self.update_status_msg = ''  # log 에 출력할 메세지 저장 멤버
            self.return_status_msg = ''  # log 에 출력할 메세지 저장 멤버
            
            # 종목 코드 및 뷰 모델 초기화
            log.info("종목 코드 및 뷰 모델 초기화 시작")
            self.sv_code_df = pd.DataFrame()
            self.db_code_df = pd.DataFrame()
            self.sv_view_model = None
            self.db_view_model = None
            log.info("종목 코드 및 뷰 모델 초기화 완료")
            
            # self.delete_outTime_column()
            
            # 동시 실행 제어 세마포어 및 이벤트 루프 설정
            log.info("세마포어 및 이벤트 루프 초기화 시작")
            self.semaphore = asyncio.Semaphore(1)  # 동시에 실행할 수 있는 최대 호출 수 설정
            self.loop = asyncio.get_event_loop()
            log.info("세마포어 및 이벤트 루프 초기화 완료")

            # 비동기 초기화 실행
            log.info("비동기 초기화 시작")
            self.loop.run_until_complete(self.initialize())
            log.info("비동기 초기화 완료")

            log.info("MainWindow 초기화 완료")
        except Exception as e:
            log.error(f"MainWindow 초기화 중 오류 발생: {e}")
            traceback.print_exc(file=sys.stdout)

    def initialize_logger(self):
        log.info("로거가 정상적으로 초기화되었습니다.")

    async def initialize(self):
        try:
            log.info("MainWindow 비동기 초기화 시작")
            await self.code_name_list_update()  # 종목 코드 업데이트
            log.info("MainWindow 비동기 초기화 완료")
        except Exception as e:
            log.error(f"initialize 중 오류 발생: {e}")
            traceback.print_exc(file=sys.stdout)
            
        print(f"종목코드 및 종목명 업데이트 완료")
        
        self.db_name = ['test_sp_1min', 'test_sp_day']

        for db_name in self.db_name:
            self.db_name = db_name
            self.connect_code_list_view()
            await self.update_price_db()
            if self.db_name == 'test_sp_day':
                print("======== 시간외 단일가 수집 중 입니다. ========")
                await self.schedule_outTime()
                print("======== 시간외 단일가 수집완료 ========")
                await self.bot.send(f"[수집기-TEST] 시간외 업데이트 완료")
                self.verify_test_db()
 
    async def code_name_list_update(self):
        # 1. API 서버에서 종목코드와 종목명 가져오기
        sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
        sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
        sv_market_list = list(map(self.objCodeMgr.get_market_kind, sv_code_list))
        sv_status_list  = list(map(self.objCodeMgr.get_code_status, sv_code_list))
        # 소속부 0:구분없음, 1:거래소, 2:코스닥, 3:K-OTC, 4:KRM, 5:KONEX  종목상태 : 0:정상, 1:거래정지, 2:거래중단
        # 1:거래소, 2:코스닥만 필터링
        filtered_indices = [i for i, kind in enumerate(sv_market_list) if kind in [1, 2]]
        sv_code_list = [sv_code_list[i] for i in filtered_indices]
        sv_name_list = [sv_name_list[i] for i in filtered_indices]
        sv_market_list = [sv_market_list[i] for i in filtered_indices]
        sv_status_list = [sv_status_list[i] for i in filtered_indices]
        
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
        
        # 현재 날짜의 연월일을 정수로 설정
        latest_date = available_latest_date()
        latest_date = latest_date // 10000
        
        # 3. MongoDB에 데이터 upsert 하지만 오늘 날짜에 이미 업데이트된 항목은 제외
        for idx, row in self.sv_code_df.iterrows():
            condition = {'stock_code': row['종목코드']}
            
            # 현재 날짜에 이미 업데이트된 항목 제외
            existing_item = self.db_handler.find_item(condition, db_name='test_sp_common', collection_name='sp_all_code_name')
            if existing_item and existing_item.get('date') == latest_date:
                continue
                
            update_value = {
                '$set': {
                    'date': latest_date,
                    'stock_name': row['종목명'],
                    'stock_code': row['종목코드'],
                    'market_kind': row['소속부'],
                    'stock_status': row['종목상태']
                },
                '$setOnInsert': {
                    'test_sp_1min': None,
                    'test_sp_day': None,
                    'sp_week': None,
                    'sp_month': None
                }
            }
            self.db_handler.upsert_item(condition, update_value, db_name='test_sp_common', collection_name='sp_all_code_name')
        
        # 4. Local MongoDB의 test_sp_day DB에서 종목코드 컬렉션 목록 가져오기
        local_code_list = self.db_handler.list_collections('test_sp_day')
        # 5. sp_all_code_name 컬렉션에서 현재 존재하는 종목코드 목록 가져오기
        existing_codes = self.db_handler.find_items_distinct(db_name='test_sp_common', collection_name='sp_all_code_name', distinct_col='stock_code')
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
                        'test_sp_1min': None,
                        'test_sp_day': None,
                        'sp_week': None,
                        'sp_month': None
                    }
                }
                self.db_handler.upsert_item(condition, update_value, db_name='test_sp_common', collection_name='sp_all_code_name')

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
            if self.db_name == 'test_sp_1min':
                print("======== 1min 수집중 입니다.========")
            elif self.db_name == 'test_sp_day':
                print("======== 일봉 수집중 입니다.======== ")
            else:
                print("분류가 없는 데이터 입니다.")
        
        self.db_code_df = pd.DataFrame(
                {'종목코드': db_code_list, '종목명': db_name_list, '갱신날짜': db_latest_list},
                columns=('종목코드', '종목명', '갱신날짜'))

        # test_sp_common DB에서 latest_date와 동일한 self.db_name 컬럼의 데이터를 가져와서 self.db_code_df에 추가
        latest_date = available_latest_date()
        if latest_date is not None:
            if self.db_name == 'test_sp_1min':
                latest_date = latest_date // 10000
            additional_codes = self.db_handler.find_items(
                {self.db_name: latest_date},
                db_name='test_sp_common',
                collection_name='sp_all_code_name',
                projection={'stock_code': 1, 'stock_name': 1, self.db_name: 1, '_id': 0}
            )
            
            if additional_codes:
                additional_codes_df = pd.DataFrame(additional_codes)
                additional_codes_df.rename(columns={self.db_name: '갱신날짜', 'stock_code': '종목코드', 'stock_name': '종목명'}, inplace=True)
                
                # test_sp_1min의 경우, date 값을 1530으로 변환
                if self.db_name == 'test_sp_1min':
                    additional_codes_df['갱신날짜'] = additional_codes_df['갱신날짜'].apply(lambda x: int(str(x) + '1530'))

                # 기존 데이터프레임에 추가 데이터프레임을 병합하고 중복 종목 코드를 제거하여 최신 값으로 대체
                self.db_code_df = pd.concat([self.db_code_df, additional_codes_df], ignore_index=True).drop_duplicates(subset='종목코드', keep='last')

        print(f"당일 업데이트 완료된 데이터 및 건수: {len(self.db_code_df)}")
        print(self.db_code_df)
    
    # update_price_db() 메서드 내 수정
    async def update_price_db(self):
        fetch_code_df = self.sv_code_df[
            (self.sv_code_df['종목상태'] == 0) & ~self.sv_code_df['종목코드'].str.startswith('Q')
        ].head(30)  # 앞에서 100개 종목만 테스트

        db_code_df = self.db_code_df

        if self.db_name == 'test_sp_1min':
            tick_unit = '분봉'
            count = 10000  # 성능을 위해 10000으로 제한
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value']
        elif self.db_name == 'test_sp_day':
            tick_unit = '일봉'
            count = 14
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value', 'marketC']
        else:
            raise ValueError("Invalid database name provided")

        latest_date = available_latest_date()
        if latest_date is not None:
            if tick_unit == '일봉':
                latest_date = latest_date // 10000
                    
            already_up_to_date_codes = db_code_df[db_code_df['갱신날짜'] == latest_date]['종목코드'].values

            if already_up_to_date_codes.size > 0:
                fetch_code_df = fetch_code_df[~fetch_code_df['종목코드'].isin(already_up_to_date_codes)]

        if fetch_code_df.empty:
            print(f"업데이트 할 종목 없음")
        else:
            print(f"업데이트 필요 종목(fetch_code_df): {fetch_code_df}")

        cnt_fetch_code_df = len(fetch_code_df)
        if self.db_name == 'test_sp_1min':
            await self.bot.send(f"[수집기-TEST] 분봉 업데이트 시작: {cnt_fetch_code_df}개")
        elif self.db_name == 'test_sp_day':
            await self.bot.send(f"[수집기] 일봉 업데이트 시작: {cnt_fetch_code_df}개")

        tqdm_range = tqdm.tqdm(total=len(fetch_code_df), ncols=100)

        tasks = []
        for i in range(len(fetch_code_df)):
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code['종목코드'], code['종목명'])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))
            tasks.append(self.update_price_for_code(code, tick_unit, count, columns, tick_range, latest_date, tqdm_range))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                print(f"예외 발생: {result}")

        tqdm_range.close()

        if self.db_name == 'test_sp_1min':
            print(f"======== 분봉 가격 데이터 수집 완료 ========")
            await self.bot.send(f"[수집기] 분봉 업데이트 완료")
        elif self.db_name == 'test_sp_day':
            print(f"======== 일봉 가격 데이터 수집 완료 ========")
            await self.bot.send(f"[수집기] 일봉 업데이트 완료")

    # 자동 검증 함수 수정 (ID 비교 제외)
    def verify_test_db(self):
        collections = self.db_handler.list_collections('test_sp_day')
        mismatches = []

        for code in collections:
            test_data = self.db_handler.find_items({}, 'test_sp_day', code)
            original_data = self.db_handler.find_items({}, 'sp_day', code)

            test_dict = {item['date']: item for item in test_data}
            original_dict = {item['date']: item for item in original_data}

            for date, test_record in test_dict.items():
                original_record = original_dict.get(date)
                if original_record is None:
                    mismatches.append((code, date, '원본 데이터 없음', test_record))
                    continue

                for field in test_record.keys():
                    if field == "_id":
                        continue  # _id 필드는 비교에서 제외
                    
                    test_value = test_record.get(field, 'N/A')
                    original_value = original_record.get(field, 'N/A')
                    if test_value != original_value:
                        mismatches.append((code, date, field, test_value, original_value))

        if mismatches:
            print("불일치 발견: 총 {}건".format(len(mismatches)))
            print("상세 비교 결과:")
            for mismatch in mismatches:
                print("종목코드: {}, 날짜: {}, 컬럼: {}, 테스트 DB 값: {}, 원본 DB 값: {}".format(*mismatch))
        else:
            print("모든 데이터가 원본 DB와 일치합니다.")

    # MongoDB 저장 시 0 또는 None 값 무시하는 로직 적용
    async def update_price_for_code(self, code, tick_unit, count, columns, tick_range, latest_date, tqdm_range):
        async with self.semaphore:
            from_date = 0
            if code['종목코드'] in self.db_code_df['종목코드'].tolist():
                latest_date_entry = self.db_handler.find_item({}, self.db_name, code['종목코드'], sort=[('date', -1)])
                from_date = latest_date_entry['date'] if latest_date_entry else 0
            tqdm_range.set_description(f"{code['종목명']}({code['종목코드']}) 처리")

            if tick_unit == '분봉':
                success = await self.objStockChart.RequestMT(code['종목코드'], 'm', tick_range, count, self, from_date)
            elif tick_unit == '일봉':
                success = await self.objStockChart.RequestDWM(code['종목코드'], 'D', count, self, from_date)

            if not success or 'date' not in self.rcv_data or len(self.rcv_data['date']) == 0:
                tqdm_range.set_description(f"{code['종목명']}({code['종목코드']}) 데이터 없음")
                tqdm_range.update(1)
                return

            df = pd.DataFrame(self.rcv_data, columns=columns, index=self.rcv_data['date'])
            df = df.loc[:from_date].iloc[:-1] if from_date != 0 else df
            df = df.iloc[::-1]
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'date'}, inplace=True)

            df.drop_duplicates(subset='date', keep='last', inplace=True)

            operations = []
            for rec in df.to_dict('records'):
                existing_data = self.db_handler.find_item({'date': rec['date']}, self.db_name, code['종목코드'])
                update_data = {}
                for key in rec.keys():
                    if key in ['date', '_id']:  
                        continue
                    # 기존 데이터가 있고, 새로 들어온 데이터가 기존과 다르면 업데이트
                    if existing_data:
                        if rec[key] != existing_data.get(key):
                            update_data[key] = rec[key]
                    else:
                        update_data[key] = rec[key]  # 기존 데이터가 없으면 저장

                if update_data:
                    operations.append(UpdateOne({'date': rec['date']}, {'$set': update_data}, upsert=True))

            if operations:
                self.db_handler._client[self.db_name][code['종목코드']].bulk_write(operations, ordered=False)

            tqdm_range.set_description(f"{code['종목명']}({code['종목코드']}) 완료")
            tqdm_range.update(1)

    async def schedule_outTime(self):
        # 현재 시간을 확인
        current_time = datetime.now()
        target_time = datetime.combine(current_time.date(), dt_time(18, 1))
        today_weekday = current_time.weekday()

        # 주말이면 바로 실행
        if today_weekday >= 0:  # 무조건실행
        # if today_weekday >= 5:  # Saturday or Sunday
            await self.handle_outTime()
        else:
            # 현재 시간이 오후 6시 1분 이후인지 확인
            if (current_time.time() >= dt_time(18, 1)):
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

    # test_sp_day 에 marketC 컬럼을 추가
    async def update_marketC_col(self):
        
        fetch_code_df = self.sv_code_df # API 서버에 있는 종목코드, 종목명 리스트
        db_code_df = self.db_code_df # DB 에 있는 종목코드, 종목명 리스트
        
        tick_unit = '일봉'
        count = 10000  # 10000개면 현재부터 1980년 까지의 데이터에 해당함. 충분.
        tick_range = 1
        columns=['marketC'] # 2024.05.05
        
        latest_date = available_latest_date() # 최근 영업일 202405051030 형식
        if latest_date is not None:
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
            # await self.objStockChart.apply_delay()
            # start_time = time.time()
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code['종목코드'], code['종목명'])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))
            tasks.append(self.update_marketC_for_code(code, tick_unit, count, columns, tick_range, tqdm_range))

        await asyncio.gather(*tasks)
        tqdm_range.close()

    async def update_marketC_for_code(self, code, tick_unit, count, columns, tick_range, tqdm_range):
        async with self.semaphore:
            # await self.objStockChart.apply_delay()
            from_date = 0
            if code['종목코드'] in self.db_code_df['종목코드'].tolist():
                # marketC 컬럼이 있는 문서 중 가장 최신의 날짜를 찾음
                latest_date_entry = self.db_handler.find_item(
                    {'marketC': {'$exists': True}}, 
                    self.db_name, 
                    code['종목코드'], 
                    sort=[('date', -1)],
                    projection={'date': 1}
                )
                from_date = latest_date_entry['date'] if latest_date_entry else 0
            if tick_unit == '일봉':  # 일봉 데이터 받기
                success = await self.objStockChart.RequestDWM(code['종목코드'], 'D', count, self, from_date)
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
        all_collections = self.db_handler._client['test_sp_day'].list_collection_names()
        # 제외할 collection 이름들
        exclude_collections = {'U001', 'U201'}
        
        # test_sp_common DB의 sp_all_code_name 컬렉션에서 stock_code가 test_sp_day의 컬렉션명인 데이터 조회
        for collection in all_collections:
            stock_info = self.db_handler.find_item(
                {'stock_code': collection}, 
                db_name='test_sp_common', 
                collection_name='sp_all_code_name', 
                projection={'stock_status': 1, '_id': 0}
            )
            if stock_info and stock_info.get('stock_status', 0) != 0:
                exclude_collections.add(collection)
        
        # Q로 시작하는 컬렉션을 exclude_collections에 추가 2025.03.06
        exclude_collections.update({col for col in all_collections if col.startswith('Q')})

        # stock_status 가 Q 가 포함이 되지 않고, 0 이 아닌 종목을 제외한 collection 이름 목록을 생성합니다.
        collections = [col for col in all_collections if col not in exclude_collections]
        
        outTimeData = []
        for code in collections:
            # if not is_market_open(): # 장 중이 아니라면
            price_latest = self.db_handler.find_item({}, 'test_sp_day', code, sort=[('date', -1)]) 
            price_lastest_date = price_latest['date'] # DB 의 최근 일봉 가격 업데이트 날짜

            latest_entry_with_diff_rate = self.db_handler.find_item(
                {'diff_rate': {'$exists': True}}, 'test_sp_day', code, sort=[('date', -1)]
            )

            if latest_entry_with_diff_rate and latest_entry_with_diff_rate['date'] >= price_lastest_date:
                continue

            condition = {'stock_code': code}
            stock_name = self.db_handler.find_item(
                condition, 
                db_name='test_sp_common', 
                collection_name='sp_all_code_name',
                projection={'stock_name': 1, '_id': 0}
            )
            outTimeData.append({'종목코드': code, '종목명': stock_name['stock_name'] if stock_name else None})

        fetch_code_df = pd.DataFrame(outTimeData)
        cnt_fetch_code_df = len(fetch_code_df)
        print("시간외 업데이트 필요한 종목의 수 (fetch_code_df) : ", cnt_fetch_code_df)
        await self.bot.send(f"[수집기-TEST] 시간외 업데이트 시작: {cnt_fetch_code_df}개")
        
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
            # await self.objStockUniWeek.apply_delay()
            from_date = 0
            # diff_rate가 없는 가장 최신의 date를 찾음
            latest_entry_with_diff_rate = self.db_handler.find_item({'diff_rate': {'$exists': True}}, 'test_sp_day', code['종목코드'], sort=[('date', -1)])
            # 해당 종목코드의 데이터 중 가장 오래된 날짜를 찾음
            earliest_entry = self.db_handler.find_item({}, 'test_sp_day', code['종목코드'], sort=[('date', 1)])
            
            # 데이터가 존재하지 않으면 diff_rate를 요청하지 않음
            if not earliest_entry:
                tqdm_range.set_description(f"[{code['종목명']}({code['종목코드']})] 데이터 없음")
                tqdm_range.update(1)
                return

            if latest_entry_with_diff_rate:
                from_date = latest_entry_with_diff_rate['date']
            else:
                from_date = earliest_entry['date']

            success = await self.objStockUniWeek.request_stock_data(code['종목코드'], count, self, from_date)

            if not success:
                tqdm_range.set_description(f"[{code['종목명']}({code['종목코드']})] 데이터 없음")
                tqdm_range.update(1)
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
                    self.db_handler._client['test_sp_day'][code['종목코드']].bulk_write(operations, ordered=False)
            
            del df
            gc.collect()
            tqdm_range.set_description(f"{code['종목명']}({code['종목코드']}) 업데이트 완료")
            tqdm_range.update(1)  # 한 종목 코드 완료 시 프로그레스바 업데이트
        
    # test_sp_day 의 특정 날짜의 수집 데이터 삭제하기
    def delete_outTime_column(self):
        target_date = 20250306
        collections = self.db_handler.list_collections("test_sp_day")
        print(f"test_sp_day collections: {collections}")
        for collection in collections:
            condition = {"date": target_date}
            result = self.db_handler.delete_items(condition, db_name="test_sp_day", collection_name=collection)
            print(f"Deleted {result.deleted_count} documents from collection {collection} in test_sp_day")

        test_sp_1min_collections = self.db_handler.list_collections("test_sp_1min")
        print(f"test_sp_1min collections: {test_sp_1min_collections}")
        for collection in test_sp_1min_collections:
            condition = {"date": {"$gte": target_date * 10000, "$lt": (target_date + 1) * 10000}}
            result = self.db_handler.delete_items(condition, db_name="test_sp_1min", collection_name=collection)
            print(f"Deleted {result.deleted_count} documents from collection {collection} in test_sp_1min")

if __name__ == "__main__":
    try:
        log.info("dataCrawler.py 실행 시작")
        log.info(f"현재 작업 디렉터리: {os.getcwd()}")
        
        # MainWindow 클래스 실행
        main_window = MainWindow()
        
        log.info("dataCrawler.py 실행 완료")
    except Exception as e:
        log.error("dataCrawler.py 실행 중 예외 발생:")
        log.error(f"{traceback.format_exc()}")

    # API 호출이 끝난 이후 다음 코드 추가
    pythoncom.CoUninitialize()