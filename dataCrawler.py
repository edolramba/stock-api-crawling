import sys
import os
import io
import gc
import asyncio
import pandas as pd
import tqdm
from datetime import datetime, timedelta, time as dt_time
from time import sleep
from common.loggerConfig import setup_logger, setup_mongodb_monitor_logger, setup_mongodb_command_logger
from util.MongoDBHandler import MongoDBHandler
from util.utils import is_market_open, available_latest_date, preformat_cjk
from pymongo import UpdateOne, monitoring, MongoClient
import pymongo
import subprocess
import time
import traceback
import numpy as np
import threading
import argparse
import ctypes
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import mplfinance as mpf
import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from bson.errors import InvalidBSON
from bson import Int64

# 표준 출력 및 에러 출력 UTF-8로 설정
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

log = setup_logger()  # 로거 설정
mongodb_monitor_log = setup_mongodb_monitor_logger()
mongodb_command_log = setup_mongodb_command_logger()

class CommandLogger(monitoring.CommandListener):
    def __init__(self, logger):
        self.log = logger
        self.last_log_time = {}
        self.log_interval = 30  # 30초
        self.lock = threading.Lock()
        # 로깅할 명령어 목록 제한
        self.loggable_commands = {
            'aggregate',  # 집계 작업
            'find',       # 조회 작업
            'update',     # 업데이트 작업
            'insert'      # 삽입 작업
        }

    def _should_log(self, key):
        now = time.time()
        with self.lock:
            last = self.last_log_time.get(key, 0)
            if now - last > self.log_interval:
                self.last_log_time[key] = now
                return True
            if now - last < 0.2:
                return False
            return False

    def started(self, event):
        # 특정 명령어만 로깅
        if event.command_name in self.loggable_commands and self._should_log(event.command_name):
            mongodb_command_log.debug(f"Command {event.command_name} started on {event.database_name}")
        
    def succeeded(self, event):
        # 특정 명령어만 로깅
        if event.command_name in self.loggable_commands and self._should_log(event.command_name):
            mongodb_command_log.debug(f"Command {event.command_name} succeeded on {event.database_name}")
        
    def failed(self, event):
        # 실패한 명령어는 항상 로깅
        mongodb_command_log.error(f"Command {event.command_name} failed on {event.database_name}")

# MongoDB 모니터링 설정
monitoring.register(CommandLogger(log))

class MainWindow:
    def __init__(self, validation_mode=False):
        try:
            log.info("MainWindow 초기화 시작")

            # validation_mode 설정
            self.validation_mode = validation_mode
            log.info(f"실행 모드: {'검증' if self.validation_mode else '수집'}")

            # MongoDB 서비스 상태 확인 및 시작
            self.ensure_mongodb_running()
            
            # MongoDBHandler 초기화
            log.info("MongoDBHandler 초기화 시작")
            self.db_handler = MongoDBHandler()
            log.info("MongoDBHandler 초기화 완료")

            # 검증 모드가 아닐 때만 Creon API 관련 초기화
            if not self.validation_mode:
                # Creon API 관련 모듈 동적 import
                try:
                    from api.creonAPI import CpStockChart, CpCodeMgr, CpStockUniWeek
                    from util.autoLogin import autoLogin
                    from util.alarm.selfTelegram import selfTelegram

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
                except ImportError as e:
                    log.error(f"Creon API 모듈 import 실패: {e}")
                    raise
            else:
                log.info("검증 모드: Creon API 초기화 건너뜀")
                self.autoLogin = None
                self.bot = None
                self.objStockChart = None
                self.objCodeMgr = None
                self.objStockUniWeek = None

            # 공통 변수 초기화
            self.rcv_data = dict()
            self.rcv_data2 = dict()
            self.update_status_msg = ''
            self.return_status_msg = ''
            
            # 종목 코드 및 뷰 모델 초기화
            log.info("종목 코드 및 뷰 모델 초기화 시작")
            self.sv_code_df = pd.DataFrame()
            self.db_code_df = pd.DataFrame()
            self.sv_view_model = None
            self.db_view_model = None
            log.info("종목 코드 및 뷰 모델 초기화 완료")
            
            # 동시 실행 제어 세마포어 및 이벤트 루프 설정
            log.info("세마포어 및 이벤트 루프 초기화 시작")
            self.semaphore = asyncio.Semaphore(1)
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

    def __del__(self):
        """소멸자에서 COM 객체 정리"""
        try:
            if hasattr(self, 'objStockChart'):
                del self.objStockChart
            if hasattr(self, 'objCodeMgr'):
                del self.objCodeMgr
            if hasattr(self, 'objStockUniWeek'):
                del self.objStockUniWeek
            if hasattr(self, 'autoLogin'):
                del self.autoLogin
            gc.collect()
        except Exception as e:
            log.error(f"COM 객체 정리 중 오류 발생: {e}")

    async def initialize(self):
        try:
            log.info("MainWindow 비동기 초기화 시작")
            
            if self.validation_mode:
                log.info("데이터 검증 모드로 실행")
                await self.validate_data_integrity()
            else:
                log.info("데이터 수집 모드로 실행")
                await self.code_name_list_update()
                
                # db_code_df 초기화
                self.db_code_df = pd.DataFrame({
                    '종목코드': [],
                    '종목명': [],
                    '갱신날짜': []
                })
                
                self.db_name = ['sp_day', 'sp_1min']
                for db_name in self.db_name:
                    self.db_name = db_name
                    try:
                        collections = self.db_handler._client[db_name].list_collection_names()
                        for collection in collections:
                            try:
                                indexes = self.db_handler._client[db_name][collection].index_information()
                                if 'date_1' not in indexes:
                                    self.db_handler._client[db_name][collection].create_index('date', name='date_1')
                                    log.info(f"Index on 'date' created for {collection}")
                            except Exception as e:
                                log.error(f"인덱스 생성 중 오류 발생 ({collection}): {str(e)}")
                                continue
                        
                        await self.update_price_db()
                        if self.db_name == 'sp_day':
                            print("======== 시간외 단일가 수집 중 입니다. ========")
                            self.semaphore = asyncio.Semaphore(3)
                            await self.schedule_outTime()
                            print("======== 시간외 단일가 수집완료 ========")
                            if self.bot:
                                await self.bot.send(f"[수집기] 시간외 업데이트 완료")
                    except Exception as e:
                        log.error(f"DB {db_name} 처리 중 오류 발생: {str(e)}")
                        continue
            
            log.info("MainWindow 비동기 초기화 완료")
        except Exception as e:
            log.error(f"initialize 중 오류 발생: {e}")
            traceback.print_exc()

    async def code_name_list_update(self):
        try:
            # 1. API 서버에서 종목코드와 종목명 가져오기
            sv_code_list = self.objCodeMgr.get_code_list(1) + self.objCodeMgr.get_code_list(2)
            sv_name_list = list(map(self.objCodeMgr.get_code_name, sv_code_list))
            sv_market_list = list(map(self.objCodeMgr.get_market_kind, sv_code_list))
            sv_status_list = list(map(self.objCodeMgr.get_code_status, sv_code_list))
            
            # 1:거래소, 2:코스닥만 필터링
            filtered_indices = [i for i, kind in enumerate(sv_market_list) if kind in [1, 2]]
            sv_code_list = [sv_code_list[i] for i in filtered_indices]
            sv_name_list = [sv_name_list[i] for i in filtered_indices]
            sv_market_list = [sv_market_list[i] for i in filtered_indices]
            sv_status_list = [sv_status_list[i] for i in filtered_indices]
            
            self.sv_code_df = pd.DataFrame({
                '종목코드': sv_code_list, 
                '종목명': sv_name_list, 
                '소속부': sv_market_list, 
                '종목상태': sv_status_list
            })
            
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
            
            print(f"종목코드 및 종목명 업데이트 완료")
            
        except Exception as e:
            log.error(f"종목코드 및 종목명 업데이트 중 오류 발생: {e}")
            traceback.print_exc()

    async def update_price_db(self):
        fetch_code_df = self.sv_code_df[
            (self.sv_code_df['종목상태'] == 0) & ~self.sv_code_df['종목코드'].str.startswith('Q')
        ]

        if self.db_name == 'sp_1min':
            tick_unit = '분봉'
            count = 200000
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value']
            self.semaphore = asyncio.Semaphore(3)
        elif self.db_name == 'sp_day':
            tick_unit = '일봉'
            count = 100
            tick_range = 1
            columns = ['open', 'high', 'low', 'close', 'volume', 'value', 'marketC']
            self.semaphore = asyncio.Semaphore(2)
        else:
            raise ValueError("Invalid database name provided")

        latest_date = available_latest_date()
        if latest_date is not None:
            if tick_unit == '일봉':
                latest_date = latest_date // 10000
                log.info(f"일봉 데이터 수집 - 최신 날짜: {latest_date}")
                
            already_up_to_date_codes = self.db_code_df[self.db_code_df['갱신날짜'] == latest_date]['종목코드'].values
            if already_up_to_date_codes.size > 0:
                fetch_code_df = fetch_code_df[~fetch_code_df['종목코드'].isin(already_up_to_date_codes)]
                log.info(f"이미 최신 데이터가 있는 종목 수: {len(already_up_to_date_codes)}")

        if fetch_code_df.empty:
            log.info("업데이트할 종목이 없습니다.")
            return

        total_count = len(fetch_code_df)
        log.info(f"업데이트할 종목 수: {total_count}")
        tqdm_range = tqdm.tqdm(total=total_count, ncols=100, desc="전체 진행률")

        start_time = time.time()
        completed = 0
        tasks = []
        results = []
        for i in range(total_count):
            code = fetch_code_df.iloc[i]
            self.update_status_msg = '[{}] {}'.format(code['종목코드'], code['종목명'])
            tqdm_range.set_description(preformat_cjk(self.update_status_msg, 25))
            # 각 종목별로 바로 실행하지 않고, 순차적으로 await
            result = await self.update_price_for_code(code, tick_unit, count, tick_range, latest_date, tqdm_range)
            results.append(result)
            completed += 1
            elapsed = time.time() - start_time
            avg_time = elapsed / completed if completed > 0 else 0
            eta = avg_time * (total_count - completed)
            tqdm_range.set_postfix({
                '완료': f"{completed}/{total_count}",
                'ETA': f"{int(eta // 60)}분 {int(eta % 60)}초"
            })
            tqdm_range.update(1)

        tqdm_range.close()

        if self.db_name == 'sp_1min':
            log.info("분봉 가격 데이터 수집 완료")
            await self.bot.send(f"[수집기] 분봉 업데이트 완료")
        elif self.db_name == 'sp_day':
            log.info("일봉 가격 데이터 수집 완료")
            await self.bot.send(f"[수집기] 일봉 업데이트 완료")

    async def check_mongodb_latest_date(self, code, latest_date):
        """MongoDB에서 종목의 최신 데이터 날짜를 확인하는 비동기 함수"""
        try:
            latest_entry = self.db_handler.find_item(
                {}, 
                self.db_name, 
                code, 
                sort=[('date', -1)],
                projection={'date': 1, '_id': 0}
            )
            
            # 일봉의 경우 날짜 형식을 맞춤
            if self.db_name == 'sp_day' and latest_entry:
                latest_entry_date = latest_entry['date']
                target_date = latest_date
            else:
                latest_entry_date = latest_entry['date'] if latest_entry else None
                target_date = latest_date
            
            if latest_entry and latest_entry_date == target_date:
                return code, True, None  # 최신 데이터 있음
            else:
                if latest_entry:
                    log.info(f"종목 {code}: DB 최신 날짜 {latest_entry_date}, 목표 날짜 {target_date}")
                return code, False, None  # 업데이트 필요
        except Exception as e:
            return code, False, e  # 오류 발생

    async def get_price_data(self, code, tick_unit, count, tick_range, latest_date):
        """가격 데이터를 조회하는 메서드"""
        try:
            log.info(f"[{code['종목코드']}] {tick_unit} 데이터 조회 시작")
            
            # API 호출 전 딜레이 추가 (4초로 증가)
            await asyncio.sleep(4)
            
            if tick_unit == '분봉':
                chunk_size = 50000
                log.info(f"[{code['종목코드']}] 분봉 데이터 요청: {min(count, chunk_size)}개")
                success = await self.objStockChart.RequestMT(
                    code['종목코드'], 'm', tick_range, 
                    min(count, chunk_size), self, 0
                )
            elif tick_unit == '일봉':
                log.info(f"[{code['종목코드']}] 일봉 데이터 요청: {count}개")
                success = await self.objStockChart.RequestDWM(
                    code['종목코드'], 'D', count, self, 0
                )
            else:
                log.error(f"[{code['종목코드']}] 잘못된 tick_unit: {tick_unit}")
                return None, "잘못된 tick_unit"

            # API 호출 후 딜레이 추가 (2초로 증가)
            await asyncio.sleep(2)

            if not success:
                log.warning(f"[{code['종목코드']}] API 호출 실패")
                return None, "API 호출 실패"
            
            if 'date' not in self.rcv_data:
                log.warning(f"[{code['종목코드']}] date 컬럼 없음")
                return None, "date 컬럼 없음"
            
            if len(self.rcv_data['date']) == 0:
                log.warning(f"[{code['종목코드']}] 데이터 없음")
                return None, "데이터 없음"

            # API 원본 데이터 로그 (keys와 샘플)
            log.debug(f"[{code['종목코드']}] API 원본 데이터 keys: {list(self.rcv_data.keys())}")
            log.debug(f"[{code['종목코드']}] API 원본 데이터 샘플: {{k: v[:2] for k, v in self.rcv_data.items()}} => { {k: v[:2] for k, v in self.rcv_data.items()} }")

            log.info(f"[{code['종목코드']}] 데이터 조회 성공: {len(self.rcv_data['date'])}개")
            return self.rcv_data, "성공"

        except Exception as e:
            log.error(f"[{code['종목코드']}] 가격 데이터 조회 중 오류 발생: {str(e)}")
            return None, str(e)

    async def update_price_for_code(self, code, tick_unit, count, tick_range, latest_date, tqdm_range):
        """특정 종목의 가격 데이터를 업데이트합니다."""
        try:
            log.info(f"[{code['종목코드']}] 가격 데이터 업데이트 시작")
            
            # 데이터 조회
            df, result_msg = await self.get_price_data(code, tick_unit, count, tick_range, latest_date)
            if df is None:
                log.warning(f"[{code['종목코드']}] 데이터 조회 실패: {result_msg}")
                tqdm_range.update(1)
                return False, result_msg

            # 데이터프레임 생성 및 전처리
            df = pd.DataFrame(df)
            if 'date' not in df.columns:
                log.error(f"[{code['종목코드']}] date 컬럼이 없습니다.")
                tqdm_range.update(1)
                return False, "date 컬럼 없음"

            # 중복 컬럼 제거
            df = df.loc[:, ~df.columns.duplicated()]
            log.info(f"[{code['종목코드']}] 중복 컬럼 제거 후 컬럼: {df.columns.tolist()}")
            
            # date 컬럼을 정수형으로 변환
            def safe_int(x):
                try:
                    return int(str(int(float(x))))
                except Exception:
                    return int(x)
            try:
                log.debug(f"[{code['종목코드']}] date 변환 전 샘플: {df['date'].head(5).tolist()} 타입: {df['date'].dtype}")
                if self.db_name == 'sp_1min':
                    from bson import Int64
                    df['date'] = df['date'].apply(lambda x: Int64(safe_int(x)))
                else:
                    df['date'] = df['date'].apply(safe_int)
                    import numpy as np
                    df['date'] = df['date'].astype(np.int32)
                # 자리수 체크
                if self.db_name == 'sp_1min':
                    assert all(df['date'].astype(str).str.len() == 12), f"분봉 date 길이 오류: {df['date'].head()}"
                else:
                    assert all(df['date'].astype(str).str.len() == 8), f"일봉 date 길이 오류: {df['date'].head()}"
                df = df.dropna(subset=['date'])
                log.debug(f"[{code['종목코드']}] date 변환 후 샘플: {df['date'].head(5).tolist()} 타입: {df['date'].dtype}")
                log.info(f"[{code['종목코드']}] 날짜 데이터 변환 완료: {len(df)}개")
            except Exception as e:
                log.error(f"[{code['종목코드']}] 날짜 데이터 변환 중 오류 발생: {str(e)}")
                tqdm_range.update(1)
                return False, f"날짜 변환 오류: {str(e)}"

            # dict 변환 시 모든 숫자 컬럼을 파이썬 int로 변환
            def to_python_types(d):
                import numpy as np
                from bson import Int64
                out = {}
                for k, v in d.items():
                    if isinstance(v, (np.integer, np.int64, np.int32)):
                        out[k] = int(v)
                    elif isinstance(v, Int64):
                        out[k] = int(v)
                    else:
                        out[k] = v
                return out

            # MongoDB에 저장 (한 건씩 update_one)
            try:
                from tqdm import tqdm
                collection = self.db_handler._client[self.db_name][code['종목코드']]
                # DB에 이미 존재하는 date set 생성
                existing_dates = set(int(d) for d in collection.distinct('date'))
                db_dates_sample = list(existing_dates)[:10]
                db_dates_types = [type(d) for d in db_dates_sample]
                api_dates = [int(rec['date']) for rec in df.to_dict('records')]
                api_dates_types = [type(d) for d in api_dates]
                log.info(f"[{code['종목코드']}] DB에서 가져온 date 샘플: {db_dates_sample} (type: {db_dates_types})")
                log.info(f"[{code['종목코드']}] API에서 받아온 date 샘플: {api_dates[:10]} (type: {api_dates_types[:10]})")
                log.info(f"[{code['종목코드']}] DB에 이미 존재하는 date 샘플: {list(existing_dates)[:5]}")
                # 신규 저장 대상만 추출
                new_records = [rec for rec in df.to_dict('records') if int(rec['date']) not in existing_dates]
                log.info(f"[{code['종목코드']}] 신규 저장 대상 date 샘플: {[int(rec['date']) for rec in new_records[:5]]}")
                log.info(f"[{code['종목코드']}] API 데이터 N={len(api_dates)}, DB에 이미 저장된 M={len(existing_dates)}, 실제로 저장할 X={len(new_records)}")
                # 처음 10개만 상세 로그, 전체는 tqdm 프로그래스바
                log.info(f"[{code['종목코드']}] 처음 10개 데이터만 저장 시도/성공/실패 로그를 남깁니다.")
                pbar = tqdm(total=len(new_records), desc=f"{code['종목코드']} 저장 진행", ncols=80)
                success_count = 0
                fail_count = 0
                for idx, rec in enumerate(new_records):
                    rec = to_python_types(rec)
                    try:
                        if idx < 10:
                            log.info(f"[{code['종목코드']}] 저장 시도: {rec}")
                        result = collection.update_one({'date': rec['date']}, {'$set': rec}, upsert=True)
                        if result.upserted_id or result.modified_count > 0 or result.matched_count > 0:
                            if idx < 10:
                                log.info(f"[{code['종목코드']}] 저장 성공: date={rec['date']} (upserted_id={result.upserted_id}, modified={result.modified_count}, matched={result.matched_count})")
                            success_count += 1
                        else:
                            if idx < 10:
                                log.warning(f"[{code['종목코드']}] 저장 결과 없음: date={rec['date']}")
                    except Exception as e:
                        if idx < 10:
                            log.error(f"[{code['종목코드']}] 저장 실패: date={rec['date']}, 오류: {str(e)}")
                        fail_count += 1
                    pbar.update(1)
                pbar.close()
                log.info(f"[{code['종목코드']}] 저장 완료: 성공 {success_count}건, 실패 {fail_count}건")

            except Exception as e:
                log.error(f"[{code['종목코드']}] 데이터 저장 중 오류 발생: {str(e)}")
                tqdm_range.update(1)
                return False, f"데이터 저장 오류: {str(e)}"
            
        except Exception as e:
            log.error(f"[{code['종목코드']}] 가격 데이터 업데이트 중 오류 발생: {str(e)}")
            tqdm_range.update(1)
            return False, str(e)

    async def schedule_outTime(self):
        # 현재 시간을 확인
        current_time = datetime.now()
        target_time = datetime.combine(current_time.date(), dt_time(18, 0))  # 18:00으로 변경
        today_weekday = current_time.weekday()

        # 주말이면 바로 실행
        if today_weekday >= 0:  # 무조건실행
        # if today_weekday >= 5:  # Saturday or Sunday
            await self.handle_outTime()
        else:
            # 현재 시간이 오후 6시 이후인지 확인
            if (current_time.time() >= dt_time(18, 0)):  # 18:00으로 변경
                await self.handle_outTime()  # 시간외 단일가 데이터 업데이트 함수 추가
            else:
                print(f"현재 시간은 {current_time.strftime('%H:%M:%S')}입니다. 오후 6시까지 기다립니다.")
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
            tasks.append(self.update_marketC_for_code(code, tick_unit, count, tick_range, tqdm_range))

        await asyncio.gather(*tasks)
        tqdm_range.close()

    async def update_marketC_for_code(self, code, tick_unit, count, tick_range, tqdm_range):
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
            
            df = pd.DataFrame(self.rcv_data)
            if 'date' in df.columns:
                df = df.sort_values('date', ascending=False)
                df.reset_index(drop=True, inplace=True)

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
        # 현재 시간 확인 및 기준 날짜 설정
        current_time = datetime.now().time()
        outtime_data_time = dt_time(18, 0)  # 시간외 단일가 데이터 기준 시간 18:00
        
        # 최근 영업일 가져오기
        latest_date = available_latest_date()
        
        # 18:00 이전이면 무조건 전일 데이터 기준으로 수집
        if current_time < outtime_data_time:
            log.info("시간외 단일가 데이터 집계 시간(18:00) 이전입니다. 전일 데이터를 수집합니다.")
            latest_date = (latest_date - 1) // 10000 * 10000  # 전일 00시 기준으로 설정
        
        latest_date = latest_date // 10000  # YYYYMMDD 형식으로 변환
        log.info(f"시간외 단일가 데이터 수집 기준일: {latest_date}")
        
        try:
            # outtime_progress 컬렉션 생성 확인 및 생성
            if 'outtime_progress' not in self.db_handler._client['sp_common'].list_collection_names():
                try:
                    # 컬렉션 생성
                    self.db_handler._client['sp_common'].create_collection('outtime_progress')
                    log.info("outtime_progress 컬렉션이 생성되었습니다.")
                    
                    # 인덱스 생성
                    self.db_handler._client['sp_common']['outtime_progress'].create_index(
                        [('stock_code', 1), ('date', 1)],
                        unique=True,
                        name='stock_code_date_idx'
                    )
                    log.info("outtime_progress 컬렉션에 인덱스가 생성되었습니다.")
                except Exception as e:
                    log.error(f"outtime_progress 컬렉션 생성 중 오류 발생: {e}")
                    # 컬렉션이 이미 존재하는 경우에도 인덱스는 확인하고 생성
                    try:
                        indexes = self.db_handler._client['sp_common']['outtime_progress'].index_information()
                        if 'stock_code_date_idx' not in indexes:
                            self.db_handler._client['sp_common']['outtime_progress'].create_index(
                                [('stock_code', 1), ('date', 1)],
                                unique=True,
                                name='stock_code_date_idx'
                            )
                            log.info("outtime_progress 컬렉션에 인덱스가 생성되었습니다.")
                    except Exception as idx_error:
                        log.error(f"인덱스 생성 중 오류 발생: {idx_error}")
        except Exception as e:
            log.error(f"outtime_progress 컬렉션 및 인덱스 생성 중 오류 발생: {e}")
            return
        
        all_collections = self.db_handler._client['sp_day'].list_collection_names()
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
        
        # Q로 시작하는 컬렉션을 exclude_collections에 추가 2025.03.06
        exclude_collections.update({col for col in all_collections if col.startswith('Q')})

        # stock_status 가 Q 가 포함이 되지 않고, 0 이 아닌 종목을 제외한 collection 이름 목록을 생성합니다.
        collections = [col for col in all_collections if col not in exclude_collections]
        
        # 진행 상황을 저장할 컬렉션 생성
        progress_collection = self.db_handler._client['sp_common']['outtime_progress']
        
        outTimeData = []
        for code in collections:
            try:
                # 1. 가장 최근 일봉 데이터 확인
                price_latest = self.db_handler.find_item({}, 'sp_day', code, sort=[('date', -1)])
                if not price_latest:
                    continue
                
                latest_daily_date = price_latest['date']
                
                # 2. 일봉 데이터가 있는 날짜까지만 시간외 단일가 수집
                target_end_date = latest_date  # latest_date를 기준으로 설정
                if latest_daily_date < target_end_date:
                    target_end_date = latest_daily_date  # 일봉 데이터의 최신 날짜가 더 과거인 경우 그것을 기준으로 함

                # 3. sp_common에서 시간외 단일가 최신 처리 날짜 확인
                stock_info = self.db_handler.find_item(
                    {'stock_code': code},
                    db_name='sp_common',
                    collection_name='sp_all_code_name',
                    projection={'outtime_last_update': 1, 'stock_name': 1, '_id': 0}
                )
                
                last_update = stock_info.get('outtime_last_update', 0) if stock_info else 0
                stock_name = stock_info.get('stock_name', '') if stock_info else ''

                # 4. 마지막 수집일부터 일봉 데이터가 있는 날짜까지만 수집
                if last_update < target_end_date:
                    outTimeData.append({
                        '종목코드': code,
                        '종목명': stock_name,
                        'last_update': last_update,
                        'target_date': target_end_date  # 반드시 포함
                    })

            except Exception as e:
                log.error(f"[{code}] 처리 중 오류 발생: {e}")
                traceback.print_exc()
                # 예외가 발생해도 target_date를 반드시 넣어서 추가
                outTimeData.append({
                    '종목코드': code,
                    '종목명': stock_name if 'stock_name' in locals() else '',
                    'last_update': last_update if 'last_update' in locals() else 0,
                    'target_date': target_end_date if 'target_end_date' in locals() else latest_date
                })
                continue

        fetch_code_df = pd.DataFrame(outTimeData)
        cnt_fetch_code_df = len(fetch_code_df)
        print("시간외 업데이트 필요한 종목의 수 (fetch_code_df) : ", cnt_fetch_code_df)
        await self.bot.send(f"[수집기] 시간외 업데이트 시작: {cnt_fetch_code_df}개")
        
        count = 200
        tqdm_range = tqdm.tqdm(total=len(fetch_code_df), ncols=100)

        # 세마포어 값을 2로 조정 (초당 4건 제한을 고려)
        self.semaphore = asyncio.Semaphore(1)  # 2에서 1로 감소
        
        # 배치 크기를 10으로 조정
        batch_size = 10  # 15에서 10으로 감소
        for i in range(0, len(fetch_code_df), batch_size):
            batch_df = fetch_code_df.iloc[i:i + batch_size]
            tasks = []
            
            for _, code in batch_df.iterrows():
                self.return_status_msg = '[{}] {}'.format(code['종목코드'], code['종목명'])
                tqdm_range.set_description(self.return_status_msg)
                tasks.append(self.update_outTime_for_code(code, count, tqdm_range))
            
            # 배치 단위로 비동기 실행
            await asyncio.gather(*tasks)
            
            # 각 배치 처리 후 20초 대기 (API 제한 준수)
            await asyncio.sleep(20)  # 15초에서 20초로 증가
            
            # 메모리 관리를 위해 가비지 컬렉션 실행
            gc.collect()

    async def update_outTime_for_code(self, code, count, tqdm_range):
        max_retries = 3
        retry_count = 0
        base_delay = 0.5
        
        while retry_count < max_retries:
            try:
                async with self.semaphore:
                    # API 호출 전 기본 딜레이
                    await asyncio.sleep(base_delay)
                    
                    # 현재 처리 중인 종목 코드 저장
                    current_stock_code = code['종목코드']
                    
                    # 데이터 타입 변환을 위한 함수
                    def convert_to_python_type(value):
                        try:
                            if isinstance(value, (np.int32, np.int64)):
                                return int(value)
                            if isinstance(value, (np.float32, np.float64)):
                                return float(value)
                            if isinstance(value, str) and value.isdigit():
                                return int(value)
                            return value
                        except Exception as e:
                            log.error(f"타입 변환 중 오류 발생: {e}, 값: {value}, 타입: {type(value)}")
                            return value

                    # API 데이터 조회 및 처리
                    try:
                        success = await self.objStockUniWeek.request_stock_data(
                            current_stock_code, 
                            count, 
                            self, 
                            0  # from_date를 0으로 설정하여 전체 데이터 조회
                        )
                        
                        if not success:
                            tqdm_range.set_description(f"[{code['종목명']}({current_stock_code})] 데이터 없음")
                            tqdm_range.update(1)
                            return

                        df = pd.DataFrame(self.rcv_data2)
                        if 'date' in df.columns:
                            df = df.sort_values('date', ascending=False)
                            df.reset_index(drop=True, inplace=True)

                        # 데이터 정합성 검증
                        if 'date' not in df.columns or 'diff_rate' not in df.columns:
                            log.error(f"[{current_stock_code}] 필수 컬럼 누락")
                            tqdm_range.update(1)
                            return

                        # 데이터 타입 변환 및 필터링
                        df['date'] = df['date'].apply(convert_to_python_type)
                        df['diff_rate'] = df['diff_rate'].apply(convert_to_python_type)
                        target_date = convert_to_python_type(code['target_date'])
                        
                        # 날짜 필터링
                        df = df[df['date'] <= target_date]
                        if df.empty:
                            tqdm_range.set_description(f"[{code['종목명']}({current_stock_code})] 대상 데이터 없음")
                            tqdm_range.update(1)
                            return

                        df.reset_index(inplace=True, drop=True)
                        df.drop_duplicates(subset='date', keep='last', inplace=True)
                        df.dropna(subset=['date', 'diff_rate'], inplace=True)

                        collection = self.db_handler._client['sp_day'][current_stock_code]
                        
                        # 데이터 저장 전 로깅
                        log.info(f"[{current_stock_code}] 저장할 데이터 수: {len(df)}")
                        
                        try:
                            # 기존 데이터 백업
                            existing_dates = set(int(d) for d in collection.distinct('date'))
                            db_dates_sample = list(existing_dates)[:10]
                            db_dates_types = [type(d) for d in db_dates_sample]
                            api_dates = [int(rec['date']) for rec in df.to_dict('records')]
                            api_dates_types = [type(d) for d in api_dates]
                            log.info(f"[{code['종목코드']}] DB에서 가져온 date 샘플: {db_dates_sample} (type: {db_dates_types})")
                            log.info(f"[{code['종목코드']}] API에서 받아온 date 샘플: {api_dates[:10]} (type: {api_dates_types[:10]})")
                            log.info(f"[{code['종목코드']}] DB에 이미 존재하는 date 샘플: {list(existing_dates)[:5]}")
                            # 신규 저장 대상만 추출
                            new_records = [rec for rec in df.to_dict('records') if int(rec['date']) not in existing_dates]
                            log.info(f"[{code['종목코드']}] 신규 저장 대상 date 샘플: {[int(rec['date']) for rec in new_records[:5]]}")
                            log.info(f"[{code['종목코드']}] API 데이터 N={len(api_dates)}, DB에 이미 저장된 M={len(existing_dates)}, 실제로 저장할 X={len(new_records)}")
                            # 처음 10개만 상세 로그, 전체는 tqdm 프로그래스바
                            log.info(f"[{code['종목코드']}] 처음 10개 데이터만 저장 시도/성공/실패 로그를 남깁니다.")
                            pbar = tqdm(total=len(new_records), desc=f"{code['종목코드']} 저장 진행", ncols=80)
                            success_count = 0
                            fail_count = 0
                            for idx, rec in enumerate(new_records):
                                rec = to_python_types(rec)
                                try:
                                    if idx < 10:
                                        log.info(f"[{code['종목코드']}] 저장 시도: {rec}")
                                    result = collection.update_one({'date': rec['date']}, {'$set': rec}, upsert=True)
                                    if result.upserted_id or result.modified_count > 0 or result.matched_count > 0:
                                        if idx < 10:
                                            log.info(f"[{code['종목코드']}] 저장 성공: date={rec['date']} (upserted_id={result.upserted_id}, modified={result.modified_count}, matched={result.matched_count})")
                                        success_count += 1
                                    else:
                                        if idx < 10:
                                            log.warning(f"[{code['종목코드']}] 저장 결과 없음: date={rec['date']}")
                                    pbar.update(1)
                                except Exception as e:
                                    if idx < 10:
                                        log.error(f"[{code['종목코드']}] 저장 실패: date={rec['date']}, 오류: {str(e)}")
                                    fail_count += 1
                                pbar.update(1)
                            pbar.close()
                            log.info(f"[{code['종목코드']}] 저장 완료: 성공 {success_count}건, 실패 {fail_count}건")

                        except Exception as e:
                            log.error(f"[{current_stock_code}] 데이터 저장 중 오류 발생: {e}")
                            if 'existing_data' in locals() and existing_data:
                                # 데이터 복구
                                restore_operations = [
                                    UpdateOne(
                                        {'date': doc['date']},
                                        {'$set': doc},
                                        upsert=False
                                    )
                                    for doc in existing_data
                                ]
                                collection.bulk_write(restore_operations, ordered=True)
                            raise
                        
                    except Exception as e:
                        log.error(f"[{current_stock_code}] API 데이터 처리 중 오류 발생: {e}")
                        raise

                    del df
                    gc.collect()
                    tqdm_range.set_description(f"{code['종목명']}({current_stock_code}) 완료")
                    tqdm_range.update(1)
                    break
                    
            except Exception as e:
                log.error(f"[{code['종목코드']}] 처리 중 오류 발생: {e}")
                traceback.print_exc()
                retry_count += 1
                delay = base_delay * (4 ** retry_count)
                if retry_count >= max_retries:
                    log.error(f"[{code['종목코드']}] 최대 재시도 횟수 초과")
                    tqdm_range.update(1)
                    break
                await asyncio.sleep(delay)

    # sp_day 의 특정 날짜의 수집 데이터 삭제하기
    def delete_outTime_column(self):
        target_date = 20250306
        collections = self.db_handler.list_collections("sp_day")
        print(f"sp_day collections: {collections}")
        for collection in collections:
            condition = {"date": target_date}
            result = self.db_handler.delete_items(condition, db_name="sp_day", collection_name=collection)
            print(f"Deleted {result.deleted_count} documents from collection {collection} in sp_day")

        sp_1min_collections = self.db_handler.list_collections("sp_1min")
        print(f"sp_1min collections: {sp_1min_collections}")
        for collection in sp_1min_collections:
            condition = {"date": {"$gte": target_date * 10000, "$lt": (target_date + 1) * 10000}}
            result = self.db_handler.delete_items(condition, db_name="sp_1min", collection_name=collection)
            print(f"Deleted {result.deleted_count} documents from collection {collection} in sp_1min")

    # 오래된 데이터는 아카이브 컬렉션으로 이동
    def archive_old_data(self):
        cutoff_date = datetime.now() - timedelta(days=365)
        self.db_handler.update_many(
            {'date': {"$lt": cutoff_date}},
            {'$set': {'archived': True}}
        )

    # 대량의 데이터 업데이트 시 배치 처리
    def batch_update(self, operations, batch_size=1000):
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            self.db_handler._client[self.db_name][code['종목코드']].bulk_write(batch, ordered=False)

    def ensure_mongodb_running(self):
        """MongoDB 서비스가 실행 중인지 확인하고, 실행되지 않았다면 시작합니다."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # MongoDB 서비스 상태 확인
                status = subprocess.run(['sc', 'query', 'MongoDB'], capture_output=True, text=True)
                
                # 서비스 상태 확인
                if "RUNNING" not in status.stdout:
                    log.warning("MongoDB 서비스가 실행되지 않고 있습니다. 서비스를 시작합니다.")
                    
                    try:
                        # 서비스가 중지된 상태인지 확인
                        if "STOPPED" in status.stdout:
                            # 서비스가 중지된 상태면 바로 시작
                            subprocess.run(['sc', 'start', 'MongoDB'], check=True)
                        else:
                            # 서비스가 중간 상태면 강제 종료 후 시작
                            subprocess.run(['taskkill', '/F', '/IM', 'mongod.exe'], shell=True, stderr=subprocess.DEVNULL)
                            time.sleep(5)  # 프로세스가 완전히 종료될 때까지 대기
                            subprocess.run(['sc', 'start', 'MongoDB'], check=True)
                            
                        time.sleep(10)  # 서비스가 완전히 시작될 때까지 대기
                        mongodb_monitor_log.info("MongoDB 서비스가 시작되었습니다.")
                        
                        # MongoDB 연결 테스트
                        test_client = MongoDBHandler()
                        test_client._client.admin.command('ping')
                        mongodb_monitor_log.info("MongoDB 연결이 성공적으로 설정되었습니다.")
                        return True
                        
                    except subprocess.CalledProcessError as e:
                        log.error(f"MongoDB 서비스 시작 실패: {e}")
                        retry_count += 1
                        if retry_count >= max_retries:
                            log.error("최대 재시도 횟수를 초과했습니다.")
                            return False
                        time.sleep(5)
                        continue
                else:
                    # 서비스가 이미 실행 중인 경우
                    try:
                        # MongoDB 연결 테스트
                        test_client = MongoDBHandler()
                        test_client._client.admin.command('ping')
                        mongodb_monitor_log.info("MongoDB 서비스가 정상적으로 실행 중입니다.")
                        return True
                    except pymongo.errors.ConnectionFailure:
                        # 서비스는 실행 중이지만 연결할 수 없는 경우
                        log.warning("MongoDB 서비스는 실행 중이지만 연결할 수 없습니다. 서비스를 재시작합니다.")
                        subprocess.run(['taskkill', '/F', '/IM', 'mongod.exe'], shell=True, stderr=subprocess.DEVNULL)
                        time.sleep(5)
                        continue
                
            except subprocess.CalledProcessError as e:
                log.error(f"MongoDB 서비스 상태 확인 중 오류 발생: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    return False
                time.sleep(5)
            except Exception as e:
                log.error(f"MongoDB 서비스 제어 중 예상치 못한 오류 발생: {e}")
                traceback.print_exc()
                retry_count += 1
                if retry_count >= max_retries:
                    return False
                time.sleep(5)
        
        return False

    async def validate_price_data(self, code, df):
        """주가 데이터 정합성 검증"""
        try:
            # 1. 필수 컬럼 존재 여부 확인
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                log.error(f"[{code['종목코드']}] 필수 컬럼 누락")
                return False

            # 2. Null 값 확인
            if df[required_columns].isnull().any().any():
                log.error(f"[{code['종목코드']}] Null 값 존재")
                return False

            # 3. 가격 데이터 논리성 검증
            invalid_rows = df[
                (df['high'] < df['low']) |  # 고가가 저가보다 낮은 경우
                (df['open'] > df['high']) |  # 시가가 고가보다 높은 경우
                (df['open'] < df['low']) |   # 시가가 저가보다 낮은 경우
                (df['close'] > df['high']) |  # 종가가 고가보다 높은 경우
                (df['close'] < df['low'])     # 종가가 저가보다 낮은 경우
            ]
            
            if not invalid_rows.empty:
                log.error(f"[{code['종목코드']}] 가격 데이터 논리 오류 발견: \n{invalid_rows}")
                return False

            # 4. 거래량 검증
            if (df['volume'] < 0).any():
                log.error(f"[{code['종목코드']}] 거래량 데이터 오류")
                return False

            # 5. 날짜 순서 검증
            if 'date' not in df.columns:
                log.error(f"[{code['종목코드']}] date 컬럼 없음")
                return False
                
            # date 컬럼을 정수형으로 변환
            df['date'] = df['date'].astype(int)
            
            # 날짜 순서 검증 (내림차순)
            if not df['date'].is_monotonic_decreasing:
                log.error(f"[{code['종목코드']}] 날짜 순서 오류")
                # 날짜 순서 재정렬 시도
                df.sort_values('date', ascending=False, inplace=True)
                df.reset_index(drop=True, inplace=True)
                # 재정렬 후에도 순서가 맞지 않으면 오류
                if not df['date'].is_monotonic_decreasing:
                    return False

            # 6. 날짜 중복 검증
            if df['date'].duplicated().any():
                log.error(f"[{code['종목코드']}] 중복 날짜 존재")
                # 중복 제거 시도
                df.drop_duplicates(subset='date', keep='first', inplace=True)
                # 중복 제거 후에도 중복이 있으면 오류
                if df['date'].duplicated().any():
                    return False

            return True

        except Exception as e:
            log.error(f"[{code['종목코드']}] 데이터 검증 중 오류 발생: {e}")
            return False

    def validate_data_types(self, df):
        """데이터 타입 검증 및 변환"""
        try:
            # 날짜 컬럼을 정수형으로 변환
            df['date'] = df['date'].astype(int)
            
            # diff_rate를 실수형으로 변환
            if 'diff_rate' in df.columns:
                df['diff_rate'] = df['diff_rate'].astype(float)
            
            return True
        except Exception as e:
            log.error(f"데이터 타입 변환 중 오류 발생: {e}")
            return False

    async def validate_data_integrity(self):
        """전체 데이터베이스의 정합성을 검증하고 수정합니다."""
        try:
            log.info("데이터 정합성 검증 시작")
            start_time = time.time()
            
            # 검증 진행상황을 저장할 컬렉션
            progress_collection = self.db_handler._client['sp_common']['validation_progress']
            
            # 이전 검증 진행상황 확인
            last_progress = progress_collection.find_one({'_id': 'last_progress'})
            if last_progress:
                log.info("이전 검증 진행상황 발견")
                last_timestamp = last_progress.get('timestamp')
                if last_timestamp:
                    last_time = datetime.fromtimestamp(last_timestamp)
                    time_diff = datetime.now() - last_time
                    if time_diff.total_seconds() < 3600:  # 1시간 이내
                        log.info(f"이전 검증이 {time_diff.total_seconds()/60:.1f}분 전에 중단되었습니다.")
                        resume = input("이전 검증을 이어서 진행하시겠습니까? (y/n): ").lower() == 'y'
                        if resume:
                            log.info("이전 검증을 이어서 진행합니다.")
                            return await self.resume_validation(last_progress)
            
            # 새로운 검증 시작
            progress_collection.delete_many({})  # 이전 진행상황 삭제
            validation_collection = self.db_handler._client['sp_common']['data_validation_results']
            validation_collection.delete_many({})  # 이전 검증 결과 삭제
            
            # 검증 결과 저장을 위한 리스트
            validation_results = []
            
            # 전체 진행상황을 위한 프로그레스바 설정
            total_steps = 3  # 일봉, 분봉, 시간외 검증
            progress_bar = tqdm.tqdm(total=total_steps, desc="전체 검증 진행률", position=0)
            
            # 1. 일봉 데이터 검증
            daily_start_time = time.time()
            daily_results = await self.validate_daily_data()
            daily_elapsed = time.time() - daily_start_time
            validation_results.extend(daily_results)
            progress_bar.update(1)
            progress_bar.set_description(f"일봉 검증 완료 (이슈: {len(daily_results)}개, 소요시간: {daily_elapsed:.1f}초)")
            
            # 진행상황 저장
            progress_collection.update_one(
                {'_id': 'last_progress'},
                {
                    '$set': {
                        'stage': 'daily_completed',
                        'timestamp': time.time(),
                        'daily_results': len(daily_results)
                    }
                },
                upsert=True
            )
            
            # 2. 분봉 데이터 검증
            minute_start_time = time.time()
            minute_results = await self.validate_minute_data()
            minute_elapsed = time.time() - minute_start_time
            validation_results.extend(minute_results)
            progress_bar.update(1)
            progress_bar.set_description(f"분봉 검증 완료 (이슈: {len(minute_results)}개, 소요시간: {minute_elapsed:.1f}초)")
            
            # 진행상황 저장
            progress_collection.update_one(
                {'_id': 'last_progress'},
                {
                    '$set': {
                        'stage': 'minute_completed',
                        'timestamp': time.time(),
                        'minute_results': len(minute_results)
                    }
                }
            )
            
            # 3. 시간외 데이터 검증
            outtime_start_time = time.time()
            outtime_results = await self.validate_outtime_data()
            outtime_elapsed = time.time() - outtime_start_time
            validation_results.extend(outtime_results)
            progress_bar.update(1)
            progress_bar.set_description(f"시간외 검증 완료 (이슈: {len(outtime_results)}개, 소요시간: {outtime_elapsed:.1f}초)")
            
            # 전체 소요 시간 계산
            total_elapsed = time.time() - start_time
            progress_bar.set_description(f"전체 검증 완료 (총 소요시간: {total_elapsed:.1f}초)")
            progress_bar.close()
            
            # 검증 결과 저장
            if validation_results:
                validation_collection.insert_many(validation_results)
            
            # 검증 결과 요약 생성
            summary = {
                'total_issues': len(validation_results),
                'by_type': {},
                'by_severity': {},
                'timestamp': datetime.now(),
                'elapsed_times': {
                    'daily': daily_elapsed,
                    'minute': minute_elapsed,
                    'outtime': outtime_elapsed,
                    'total': total_elapsed
                }
            }
            
            for result in validation_results:
                # 이슈 타입별 집계
                issue_type = result['issue_type']
                summary['by_type'][issue_type] = summary['by_type'].get(issue_type, 0) + 1
                
                # 심각도별 집계
                severity = result['severity']
                summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # 요약 정보 저장
            validation_collection.insert_one({
                '_id': 'summary',
                'summary': summary
            })
            
            # 검증 완료 후 진행상황 삭제
            progress_collection.delete_many({})
            
            # 검증 결과 요약 출력
            log.info("\n=== 검증 결과 요약 ===")
            log.info(f"총 이슈 수: {summary['total_issues']}개")
            log.info("\n이슈 타입별 현황:")
            for issue_type, count in summary['by_type'].items():
                log.info(f"- {issue_type}: {count}건")
            log.info("\n심각도별 현황:")
            for severity, count in summary['by_severity'].items():
                log.info(f"- {severity}: {count}건")
            
            # GUI 실행
            gui = ValidationGUI(validation_results, self.db_handler)
            gui.run()
            
            log.info("데이터 정합성 검증 완료")
            return validation_results
            
        except Exception as e:
            log.error(f"데이터 정합성 검증 중 오류 발생: {str(e)}")
            traceback.print_exc()
            return []

    async def resume_validation(self, last_progress):
        """이전 검증을 이어서 진행합니다."""
        try:
            stage = last_progress.get('stage', '')
            validation_results = []
            
            # 이전 검증 결과 로드
            validation_collection = self.db_handler._client['sp_common']['data_validation_results']
            previous_results = list(validation_collection.find({'_id': {'$ne': 'summary'}}))
            validation_results.extend(previous_results)
            
            # 전체 진행상황을 위한 프로그레스바 설정
            total_steps = 3
            progress_bar = tqdm.tqdm(total=total_steps, desc="전체 검증 진행률", position=0)
            
            if stage == 'daily_completed':
                progress_bar.update(1)
                log.info("일봉 데이터 검증 결과 복원 완료")
                
                # 분봉 데이터 검증
                minute_results = await self.validate_minute_data()
                validation_results.extend(minute_results)
                progress_bar.update(1)
                
                # 시간외 데이터 검증
                outtime_results = await self.validate_outtime_data()
                validation_results.extend(outtime_results)
                progress_bar.update(1)
                
            elif stage == 'minute_completed':
                progress_bar.update(2)
                log.info("일봉, 분봉 데이터 검증 결과 복원 완료")
                
                # 시간외 데이터 검증
                outtime_results = await self.validate_outtime_data()
                validation_results.extend(outtime_results)
                progress_bar.update(1)
            
            progress_bar.close()
            
            # 검증 결과 저장
            if validation_results:
                validation_collection.insert_many(validation_results)
            
            # 검증 결과 요약 생성
            summary = {
                'total_issues': len(validation_results),
                'by_type': {},
                'by_severity': {},
                'timestamp': datetime.now()
            }
            
            for result in validation_results:
                issue_type = result['issue_type']
                summary['by_type'][issue_type] = summary['by_type'].get(issue_type, 0) + 1
                severity = result['severity']
                summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # 요약 정보 저장
            validation_collection.insert_one({
                '_id': 'summary',
                'summary': summary
            })
            
            # 검증 결과 요약 출력
            log.info("\n=== 검증 결과 요약 ===")
            log.info(f"총 이슈 수: {summary['total_issues']}개")
            log.info("\n이슈 타입별 현황:")
            for issue_type, count in summary['by_type'].items():
                log.info(f"- {issue_type}: {count}건")
            log.info("\n심각도별 현황:")
            for severity, count in summary['by_severity'].items():
                log.info(f"- {severity}: {count}건")
            
            # GUI 실행
            gui = ValidationGUI(validation_results, self.db_handler)
            gui.run()
            
            log.info("데이터 정합성 검증 완료")
            return validation_results
            
        except Exception as e:
            log.error(f"검증 재개 중 오류 발생: {str(e)}")
            traceback.print_exc()
            return []

    async def validate_daily_data(self):
        """일봉 데이터의 정합성을 검증합니다."""
        validation_results = []
        try:
            # 진행상황 불러오기
            progress_collection = self.db_handler._client['sp_common']['validation_progress']
            last_progress = progress_collection.find_one({'_id': 'daily_progress'})
            last_collection = last_progress.get('collection') if last_progress else None
            last_chunk = last_progress.get('chunk') if last_progress else None
            resume_mode = last_progress is not None

            collections = self.db_handler._client['sp_day'].list_collection_names()
            total_collections = len(collections)
            validation_collection = self.db_handler._client['sp_common']['data_validation_results']
            progress_bar = tqdm.tqdm(total=total_collections, desc="일봉 데이터 검증", position=1, leave=False)
            import multiprocessing
            max_workers = max(1, int(multiprocessing.cpu_count() * 0.8))
            semaphore = asyncio.Semaphore(max_workers)
            start_time = time.time()
            processed_collections = 0
            batch_results = []
            batch_size = 100
            # 진행상황 저장 주기
            SAVE_INTERVAL = 1000
            processed_chunks = 0
            save_flag = False

            async def process_chunk(collection, chunk_start, chunk_size, validation_results):
                nonlocal processed_chunks, save_flag
                try:
                    data = list(self.db_handler._client['sp_day'][collection].find(
                        {},
                        skip=chunk_start,
                        limit=chunk_size,
                        sort=[('date', 1)]
                    ))
                    if not data:
                        return
                    for i in range(len(data)-1):
                        current = data[i]
                        next_data = data[i+1]
                        if next_data['date'] - current['date'] != 1:
                            validation_results.append({
                                'collection': collection,
                                'date': current['date'],
                                'issue_type': 'date_gap',
                                'severity': 'warning',
                                'description': f'날짜 불연속: {current["date"]} -> {next_data["date"]}',
                                'timestamp': datetime.now()
                            })
                        if current['high'] < current['low'] or \
                           current['open'] > current['high'] or \
                           current['open'] < current['low'] or \
                           current['close'] > current['high'] or \
                           current['close'] < current['low']:
                            validation_results.append({
                                'collection': collection,
                                'date': current['date'],
                                'issue_type': 'price_inconsistency',
                                'severity': 'error',
                                'description': '가격 데이터 불일치',
                                'timestamp': datetime.now()
                            })
                        if current['volume'] < 0:
                            validation_results.append({
                                'collection': collection,
                                'date': current['date'],
                                'issue_type': 'volume_error',
                                'severity': 'error',
                                'description': '거래량이 음수',
                                'timestamp': datetime.now()
                            })
                        if 'marketC' in current and current['marketC'] < 0:
                            validation_results.append({
                                'collection': collection,
                                'date': current['date'],
                                'issue_type': 'market_cap_error',
                                'severity': 'error',
                                'description': '시가총액이 음수',
                                'timestamp': datetime.now()
                            })
                    processed_chunks += 1
                    if processed_chunks % SAVE_INTERVAL == 0:
                        progress_collection.update_one(
                            {'_id': 'daily_progress'},
                            {'$set': {'collection': collection, 'chunk': chunk_start // chunk_size}},
                            upsert=True
                        )
                        save_flag = True
                except Exception as e:
                    log.error(f"[{collection}] 청크 처리 중 오류 발생: {str(e)}")
                    validation_results.append({
                        'collection': collection,
                        'issue_type': 'validation_error',
                        'severity': 'error',
                        'description': f'청크 처리 중 오류 발생: {str(e)}',
                        'timestamp': datetime.now()
                    })

            async def process_collection(collection, idx):
                nonlocal processed_collections, batch_results
                async with semaphore:
                    try:
                        if 'date_1' not in self.db_handler._client['sp_day'][collection].index_information():
                            self.db_handler._client['sp_day'][collection].create_index('date', name='date_1')
                        total_docs = self.db_handler._client['sp_day'][collection].count_documents({})
                        if total_docs == 0:
                            log.warning(f"[{collection}] 데이터가 없습니다.")
                            processed_collections += 1
                            progress_bar.update(1)
                            return
                        chunk_size = 1000
                        chunks = [i for i in range(0, total_docs, chunk_size)]
                        # resume 모드일 때 skip 로직
                        if resume_mode:
                            if collection < last_collection:
                                processed_collections += 1
                                progress_bar.update(1)
                                return
                        for idx, chunk_start in enumerate(chunks):
                            if resume_mode:
                                if collection == last_collection and idx < last_chunk:
                                    continue
                            await process_chunk(collection, chunk_start, chunk_size, batch_results)
                        processed_collections += 1
                        if len(batch_results) >= batch_size:
                            try:
                                validation_collection.insert_many(batch_results)
                                batch_results = []
                            except Exception as e:
                                log.error(f"중간 결과 저장 중 오류 발생: {str(e)}")
                        elapsed_time = time.time() - start_time
                        avg_time_per_collection = elapsed_time / processed_collections
                        remaining_collections = total_collections - processed_collections
                        estimated_time_remaining = avg_time_per_collection * remaining_collections
                        progress_bar.set_description(
                            f"일봉 데이터 검증 ({processed_collections}/{total_collections}) "
                            f"예상 완료시간: {estimated_time_remaining:.1f}초"
                        )
                        progress_bar.update(1)
                    except Exception as e:
                        log.error(f"[{collection}] 검증 중 오류 발생: {str(e)}")
                        batch_results.append({
                            'collection': collection,
                            'issue_type': 'validation_error',
                            'severity': 'error',
                            'description': f'검증 중 오류 발생: {str(e)}',
                            'timestamp': datetime.now()
                        })
                        processed_collections += 1
                        progress_bar.update(1)
            tasks = [process_collection(col, idx+1) for idx, col in enumerate(collections)]
            await asyncio.gather(*tasks)
            if batch_results:
                try:
                    validation_collection.insert_many(batch_results)
                except Exception as e:
                    log.error(f"최종 결과 저장 중 오류 발생: {str(e)}")
            progress_bar.close()
            # 마지막 진행상황 저장
            if save_flag:
                progress_collection.update_one(
                    {'_id': 'daily_progress'},
                    {'$set': {'collection': None, 'chunk': None}},
                    upsert=True
                )
        except Exception as e:
            log.error(f"일봉 데이터 검증 중 오류 발생: {str(e)}")
            traceback.print_exc()
        return validation_results

    async def validate_minute_data(self):
        """분봉 데이터의 정합성을 검증합니다."""
        validation_results = []
        try:
            # 진행상황 불러오기
            progress_collection = self.db_handler._client['sp_common']['validation_progress']
            last_progress = progress_collection.find_one({'_id': 'minute_progress'})
            last_collection = last_progress.get('collection') if last_progress else None
            last_date = last_progress.get('date') if last_progress else None
            last_chunk = last_progress.get('chunk') if last_progress else None
            resume_mode = last_progress is not None

            collections = self.db_handler._client['sp_1min'].list_collection_names()
            # 전체 문서 수 집계
            total_docs = 0
            collection_doc_counts = []
            for collection_name in collections:
                count = self.db_handler._client['sp_1min'][collection_name].estimated_document_count()
                collection_doc_counts.append((collection_name, count))
                total_docs += count
            print(f'분봉 검증 총 문서 수: {total_docs}')
            progress_bar = tqdm.tqdm(total=total_docs, desc="분봉 데이터 검증", position=1, leave=False, ncols=100)
            
            # 병렬 처리를 위한 세마포어 설정
            import multiprocessing
            max_workers = max(1, int(multiprocessing.cpu_count() * 0.8))
            semaphore = asyncio.Semaphore(max_workers)
            
            # 진행상황 저장 주기
            SAVE_INTERVAL = 1000
            processed_chunks = 0
            save_flag = False
            
            async def process_chunk(collection, date, chunk_start, chunk_size, validation_results):
                nonlocal processed_chunks, save_flag
                try:
                    with self.db_handler._client.start_session() as session:
                        cursor = self.db_handler._client['sp_1min'][collection].find(
                            {'date': date},
                            skip=chunk_start,
                            limit=chunk_size,
                            sort=[('time', 1)],
                            batch_size=1000,
                            no_cursor_timeout=True,
                            session=session
                        )
                        prev = None
                        for doc in cursor:
                            try:
                                if prev is not None:
                                    if doc['time'] - prev['time'] != 1:
                                        validation_results.append({
                                            'collection': collection,
                                            'date': prev['date'],
                                            'time': prev['time'],
                                            'issue_type': 'time_gap',
                                            'severity': 'warning',
                                            'description': f'시간 불연속: {prev["time"]} -> {doc["time"]}',
                                            'timestamp': datetime.now()
                                        })
                                    if prev['high'] < prev['low'] or \
                                       prev['open'] > prev['high'] or \
                                       prev['open'] < prev['low'] or \
                                       prev['close'] > prev['high'] or \
                                       prev['close'] < prev['low']:
                                        validation_results.append({
                                            'collection': collection,
                                            'date': prev['date'],
                                            'time': prev['time'],
                                            'issue_type': 'price_inconsistency',
                                            'severity': 'error',
                                            'description': '가격 데이터 불일치',
                                            'timestamp': datetime.now()
                                        })
                                    if prev['volume'] < 0:
                                        validation_results.append({
                                            'collection': collection,
                                            'date': prev['date'],
                                            'time': prev['time'],
                                            'issue_type': 'volume_error',
                                            'severity': 'error',
                                            'description': '거래량이 음수',
                                            'timestamp': datetime.now()
                                        })
                                prev = doc
                                progress_bar.update(1)
                            except KeyError as e:
                                if str(e) == "'time'":
                                    validation_results.append({
                                        'collection': collection,
                                        'date': date,
                                        'issue_type': 'missing_time_field',
                                        'severity': 'error',
                                        'description': f'time 필드 누락 (date: {date}, _id: {getattr(doc, "_id", "N/A")})',
                                        'timestamp': datetime.now()
                                    })
                                else:
                                    msg = f"[{collection}] 문서 처리 중 오류: {e} (date: {date}, _id: {getattr(doc, '_id', 'N/A')})"
                                    log.error(msg)
                                    print(msg)
                                progress_bar.update(1)
                                continue
                            except InvalidBSON as e:
                                msg = f"[{collection}] BSON 오류 발생: {e} (date: {date}, _id: {getattr(doc, '_id', 'N/A')})"
                                log.error(msg)
                                print(msg)
                                progress_bar.update(1)
                                continue
                            except Exception as e:
                                msg = f"[{collection}] 문서 처리 중 오류: {e} (date: {date}, _id: {getattr(doc, '_id', 'N/A')})"
                                log.error(msg)
                                print(msg)
                                progress_bar.update(1)
                                continue
                    processed_chunks += 1
                    if processed_chunks % SAVE_INTERVAL == 0:
                        # 진행상황 저장
                        progress_collection.update_one(
                            {'_id': 'minute_progress'},
                            {'$set': {'collection': collection, 'date': date, 'chunk': chunk_start // chunk_size}},
                            upsert=True
                        )
                        save_flag = True
                except Exception as e:
                    log.error(f"[{collection}] 청크 처리 중 오류 발생: {str(e)}")
                    validation_results.append({
                        'collection': collection,
                        'date': date,
                        'issue_type': 'validation_error',
                        'severity': 'error',
                        'description': f'청크 처리 중 오류 발생: {str(e)}',
                        'timestamp': datetime.now()
                    })
            
            async def process_collection(collection, count):
                async with semaphore:
                    try:
                        if 'date_1_time_1' not in self.db_handler._client['sp_1min'][collection].index_information():
                            self.db_handler._client['sp_1min'][collection].create_index([('date', 1), ('time', 1)])
                        dates = self.db_handler._client['sp_1min'][collection].distinct('date')
                        if not dates:
                            log.warning(f"[{collection}] 데이터가 없습니다.")
                            return
                        chunk_size = 200
                        for date in dates:
                            total_docs = self.db_handler._client['sp_1min'][collection].count_documents({'date': date})
                            chunks = [i for i in range(0, total_docs, chunk_size)]
                            # resume 모드일 때 skip 로직
                            if resume_mode:
                                if collection < last_collection:
                                    continue
                                if collection == last_collection and date < last_date:
                                    continue
                            for idx, chunk_start in enumerate(chunks):
                                # resume 모드일 때 skip 로직
                                if resume_mode:
                                    if collection == last_collection and date == last_date and idx < last_chunk:
                                        continue
                                await process_chunk(collection, date, chunk_start, chunk_size, validation_results)
                    except Exception as e:
                        log.error(f"[{collection}] 검증 중 오류 발생: {str(e)}")
                        validation_results.append({
                            'collection': collection,
                            'issue_type': 'validation_error',
                            'severity': 'error',
                            'description': f'검증 중 오류 발생: {str(e)}',
                            'timestamp': datetime.now()
                        })
            tasks = [process_collection(col, count) for col, count in collection_doc_counts]
            await asyncio.gather(*tasks)
            progress_bar.close()
            # 마지막 진행상황 저장
            if save_flag:
                progress_collection.update_one(
                    {'_id': 'minute_progress'},
                    {'$set': {'collection': None, 'date': None, 'chunk': None}},
                    upsert=True
                )
            # 검증 결과 저장
            if validation_results:
                validation_collection = self.db_handler._client['sp_common']['data_validation_results']
                validation_collection.insert_many(validation_results)
        except Exception as e:
            log.error(f"분봉 데이터 검증 중 오류 발생: {str(e)}")
            traceback.print_exc()
        return validation_results

    async def validate_outtime_data(self):
        """시간외 데이터의 정합성을 검증합니다."""
        validation_results = []
        try:
            collections = self.db_handler._client['sp_day'].list_collection_names()
            for collection in collections:
                try:
                    # 1. 시간외 데이터 검증
                    data = list(self.db_handler._client['sp_day'][collection].find(
                        {'diff_rate': {'$exists': True}},
                        sort=[('date', 1)]
                    ))
                    
                    if not data:
                        continue
                        
                    # 2. 데이터 정합성 검증
                    for doc in data:
                        # diff_rate 값 검증
                        if not isinstance(doc.get('diff_rate'), (int, float)):
                            validation_results.append({
                                'collection': collection,
                                'date': doc['date'],
                                'issue_type': 'diff_rate_type_error',
                                'severity': 'error',
                                'description': 'diff_rate 타입 오류',
                                'timestamp': datetime.now()
                            })
                    
                except Exception as e:
                    log.error(f"[{collection}] 시간외 데이터 검증 중 오류 발생: {str(e)}")
                    validation_results.append({
                        'collection': collection,
                        'issue_type': 'validation_error',
                        'severity': 'error',
                        'description': f'검증 중 오류 발생: {str(e)}',
                        'timestamp': datetime.now()
                    })
                    continue
                    
        except Exception as e:
            log.error(f"시간외 데이터 검증 중 오류 발생: {str(e)}")
            
        return validation_results

    def show_validation_results(self):
        """검증 결과를 UI로 표시합니다."""
        try:
            validation_collection = self.db_handler._client['sp_common']['data_validation_results']
            
            # 요약 정보 조회
            summary = validation_collection.find_one({'_id': 'summary'})
            if not summary:
                print("검증 결과가 없습니다.")
                return
                
            summary_data = summary['summary']
            
            # 검증 결과 조회
            results = list(validation_collection.find({'_id': {'$ne': 'summary'}}))
            
            # 결과 출력
            print("\n=== 데이터 검증 결과 요약 ===")
            print(f"총 이슈 수: {summary_data['total_issues']}")
            print("\n이슈 타입별 현황:")
            for issue_type, count in summary_data['by_type'].items():
                print(f"- {issue_type}: {count}건")
            print("\n심각도별 현황:")
            for severity, count in summary_data['by_severity'].items():
                print(f"- {severity}: {count}건")
            
            print("\n=== 상세 이슈 목록 ===")
            for result in results:
                print(f"\n종목: {result['collection']}")
                print(f"날짜: {result.get('date', 'N/A')}")
                print(f"이슈 유형: {result['issue_type']}")
                print(f"심각도: {result['severity']}")
                print(f"설명: {result['description']}")
                if 'details' in result:
                    print(f"상세 정보: {result['details']}")
                print("-" * 50)
                
        except Exception as e:
            log.error(f"검증 결과 표시 중 오류 발생: {str(e)}")
            traceback.print_exc()

class ValidationGUI:
    def __init__(self, validation_results, db_handler):
        self.root = tk.Tk()
        self.root.title("데이터 검증 결과")
        self.root.geometry("1200x800")
        
        self.validation_results = validation_results
        self.db_handler = db_handler
        self.current_index = 0
        
        # 종목별로 그룹화
        self.stock_groups = {}
        for result in validation_results:
            if result['severity'] == 'error' and result['issue_type'] in ['price_inconsistency', 'time_gap']:
                stock_code = result['collection']
                if stock_code not in self.stock_groups:
                    self.stock_groups[stock_code] = []
                self.stock_groups[stock_code].append(result)
        
        self.stock_codes = list(self.stock_groups.keys())
        
        # GUI 구성
        self.create_widgets()
        
        # 첫 번째 종목 표시
        if self.stock_codes:
            self.show_current_stock()
    
    def __del__(self):
        """소멸자에서 matplotlib 객체 정리"""
        try:
            plt.close('all')
            gc.collect()
        except Exception as e:
            log.error(f"matplotlib 객체 정리 중 오류 발생: {e}")

    def create_widgets(self):
        # 상단 프레임 (종목 정보 및 버튼)
        top_frame = ttk.Frame(self.root)
        top_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 종목 정보 레이블
        self.stock_info_label = ttk.Label(top_frame, text="", font=('Arial', 12, 'bold'))
        self.stock_info_label.pack(side=tk.LEFT, padx=5)
        
        # 버튼 프레임
        button_frame = ttk.Frame(top_frame)
        button_frame.pack(side=tk.RIGHT, padx=5)
        
        # 이전/다음 버튼
        ttk.Button(button_frame, text="이전 종목", command=self.show_previous_stock).pack(side=tk.LEFT, padx=2)
        ttk.Button(button_frame, text="다음 종목", command=self.show_next_stock).pack(side=tk.LEFT, padx=2)
        
        # 차트 프레임
        self.chart_frame = ttk.Frame(self.root)
        self.chart_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 하단 프레임 (이슈 목록)
        bottom_frame = ttk.Frame(self.root)
        bottom_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # 이슈 목록
        self.issue_tree = ttk.Treeview(bottom_frame, columns=('date', 'type', 'description'), show='headings')
        self.issue_tree.heading('date', text='날짜')
        self.issue_tree.heading('type', text='이슈 유형')
        self.issue_tree.heading('description', text='설명')
        self.issue_tree.pack(fill=tk.X)
    
    def show_current_stock(self):
        if not self.stock_codes:
            return
            
        stock_code = self.stock_codes[self.current_index]
        issues = self.stock_groups[stock_code]
        
        # 종목 정보 업데이트
        self.stock_info_label.config(text=f"종목: {stock_code} ({len(issues)}개 이슈)")
        
        # 차트 업데이트
        self.update_charts(stock_code, issues)
        
        # 이슈 목록 업데이트
        self.update_issue_list(issues)
    
    def update_charts(self, stock_code, issues):
        # 기존 차트 제거
        for widget in self.chart_frame.winfo_children():
            widget.destroy()
        
        try:
            # MongoDB 데이터 가져오기
            mongo_data = list(self.db_handler._client['sp_day'][stock_code].find(
                {},
                sort=[('date', 1)]
            ))
            
            if not mongo_data:
                return
            
            # MongoDB 데이터를 DataFrame으로 변환
            mongo_df = pd.DataFrame(mongo_data)
            mongo_df['date'] = pd.to_datetime(mongo_df['date'].astype(str))
            mongo_df.set_index('date', inplace=True)
            
            # 차트 생성
            fig = Figure(figsize=(12, 8))
            
            # MongoDB 데이터 플롯
            ax1 = fig.add_subplot(111)
            mpf.plot(
                mongo_df,
                type='candle',
                style='charles',
                title=f'{stock_code} - MongoDB Data',
                ax=ax1,
                volume=True
            )
            
            # 이슈 표시
            for issue in issues:
                issue_date = pd.to_datetime(str(issue['date']))
                if issue_date in mongo_df.index:
                    ax1.axvline(x=issue_date, color='r', linestyle='--', alpha=0.5)
                    ax1.text(
                        issue_date,
                        mongo_df.loc[issue_date, 'high'],
                        f"Issue: {issue['issue_type']}",
                        rotation=45
                    )
            
            # 차트를 Tkinter에 추가
            canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
            
        except Exception as e:
            log.error(f"차트 업데이트 중 오류 발생: {str(e)}")
            traceback.print_exc()
    
    def update_issue_list(self, issues):
        # 기존 항목 제거
        for item in self.issue_tree.get_children():
            self.issue_tree.delete(item)
        
        # 새로운 이슈 추가
        for issue in issues:
            self.issue_tree.insert('', 'end', values=(
                issue['date'],
                issue['issue_type'],
                issue['description']
            ))
    
    def show_previous_stock(self):
        if self.current_index > 0:
            self.current_index -= 1
            self.show_current_stock()
    
    def show_next_stock(self):
        if self.current_index < len(self.stock_codes) - 1:
            self.current_index += 1
            self.show_current_stock()
    
    def run(self):
        try:
            self.root.mainloop()
        finally:
            # GUI 종료 시 정리
            plt.close('all')
            gc.collect()

def check_bson_error():
    client = MongoClient('mongodb://localhost:27017/')
    rag_db = client['rag']
    error_col = rag_db['bsonerror']
    db_names = [db for db in client.list_database_names() if db not in ('admin', 'local', 'config', 'rag')]
    # 전체 문서 개수 집계
    total_docs = 0
    collection_doc_counts = []
    for db_name in db_names:
        db = client[db_name]
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            count = collection.estimated_document_count()
            collection_doc_counts.append((db_name, collection_name, count))
            total_docs += count
    print(f'총 검사 대상 문서 수: {total_docs}')
    pbar = tqdm.tqdm(total=total_docs, desc='BSON 오류 검사', ncols=100)
    processed_docs = 0
    for db_name, collection_name, count in collection_doc_counts:
        db = client[db_name]
        collection = db[collection_name]
        checked_ids = set(doc['_id'] for doc in error_col.find({'db': db_name, 'collection': collection_name}, {'_id': 1}))
        cursor = collection.find({})
        for doc in cursor:
            doc_id = getattr(doc, '_id', None)
            if doc_id in checked_ids:
                pbar.update(1)
                continue
            try:
                _ = doc
            except InvalidBSON as e:
                msg = f'BSON 오류: {db_name}.{collection_name} _id={doc_id}'
                print(msg)
                error_col.update_one(
                    {'db': db_name, 'collection': collection_name, '_id': doc_id},
                    {'$set': {'errmsg': str(e)}},
                    upsert=True
                )
            except Exception as e:
                msg = f'기타 오류: {db_name}.{collection_name} _id={doc_id} {e}'
                print(msg)
                error_col.update_one(
                    {'db': db_name, 'collection': collection_name, '_id': doc_id},
                    {'$set': {'errmsg': str(e)}},
                    upsert=True
                )
            pbar.update(1)
    pbar.close()
    print('BSON 오류 검사 완료')

def delete_bson_error():
    client = MongoClient('mongodb://localhost:27017/')
    rag_db = client['rag']
    error_col = rag_db['bsonerror']
    errors = list(error_col.find({}))
    total = len(errors)
    print(f'총 삭제 대상 문서 수: {total}')
    pbar = tqdm.tqdm(total=total, desc='BSON 오류 문서 삭제', ncols=100)
    for err in errors:
        db = client[err['db']]
        col = db[err['collection']]
        print(f'삭제: {err["db"]}.{err["collection"]} _id={err["_id"]}')
        col.delete_one({'_id': err['_id']})
        # 삭제가 완료된 문서는 bsonerror에서 바로 삭제
        error_col.delete_one({'_id': err['_id'], 'db': err['db'], 'collection': err['collection']})
        pbar.update(1)
    pbar.close()
    print('BSON 오류 문서 삭제 완료')

if __name__ == "__main__":
    import argparse
    import ctypes
    import sys
    import os

    def is_admin():
        try:
            return os.name == 'nt' and ctypes.windll.shell32.IsUserAnAdmin()
        except:
            return False

    if not is_admin():
        # 관리자 권한으로 재실행
        params = ' '.join([f'"{arg}"' if ' ' in arg else arg for arg in sys.argv[1:]])
        script = sys.executable
        if sys.argv[0].endswith('.py'):
            params = f'"{sys.argv[0]}" {params}'
        else:
            params = params
        ctypes.windll.shell32.ShellExecuteW(
            None, "runas", script, params, None, 1
        )
        sys.exit()

    parser = argparse.ArgumentParser(description='주식 데이터 수집 및 검증 도구')
    parser.add_argument('--mode', choices=['collect', 'validate', 'chkbsonerror', 'deletebsonerror', 'chart'],
                      help='실행 모드: collect(데이터 수집) 또는 validate(데이터 검증) 또는 chkbsonerror(BSON 오류 검사) 또는 deletebsonerror(BSON 오류 삭제) 또는 chart(차트 뷰어)')
    
    args = parser.parse_args()
    
    try:
        log.info("dataCrawler.py 실행 시작")
        log.info(f"현재 작업 디렉터리: {os.getcwd()}")
        
        if args.mode == 'chkbsonerror':
            print("BSON 오류 검사 모드 시작")
            check_bson_error()
            print("BSON 오류 검사 완료")
        elif args.mode == 'deletebsonerror':
            print("BSON 오류 문서 삭제 모드 시작")
            delete_bson_error()
            print("BSON 오류 문서 삭제 완료")
        elif args.mode == 'chart':
            from chart_viewer import main as chart_main
            chart_main()
        else:
            # MainWindow 클래스 실행
            main_window = MainWindow(validation_mode=(args.mode == 'validate'))
        
        log.info("dataCrawler.py 실행 완료")
    except Exception as e:
        log.error("dataCrawler.py 실행 중 예외 발생:")
        log.error(f"{traceback.format_exc()}")
        print("\n[오류] 프로그램 실행 중 예외가 발생했습니다. 로그를 확인해 주세요. 창을 닫지 않고 대기합니다.")
        try:
            input("\n엔터를 누르면 프로그램이 종료됩니다.")
        except Exception:
            pass
        try:
            os.system('pause')
        except Exception:
            pass
    finally:
        gc.collect()
        # 마지막에도 pause 한 번 더
        try:
            os.system('pause')
        except Exception:
            pass
