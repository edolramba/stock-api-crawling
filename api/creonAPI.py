# coding=utf-8
import win32com.client
import time
from datetime import datetime
from typing import TYPE_CHECKING
import asyncio

if TYPE_CHECKING:
    from creon_datareader_v1_0 import MainWindow
    
g_objCpStatus = win32com.client.Dispatch('CpUtil.CpCybos')

def pump_messages():
    while True:
        msg = win32com.client.pythoncom.PumpWaitingMessages()
        if not msg:
            break
        
# original_func 콜하기 전에 PLUS 연결 상태 체크하는 데코레이터
def check_PLUS_status(original_func):
    def wrapper(*args, **kwargs):
        if not g_objCpStatus.IsConnect:
            print("PLUS가 정상적으로 연결되지 않음.")  # 연결 실패 메시지 출력
            raise ConnectionError("PLUS가 정상적으로 연결되지 않음.")  # 예외 발생
        print("already connected.")
        pump_messages()  # 메시지 루프 처리 추가
        return original_func(*args, **kwargs)
    return wrapper


# 서버로부터 과거의 차트 데이터 가져오는 클래스
class CpStockChart:
    def __init__(self):
        self.objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")
    
    def _check_rq_status(self):
        """
        self.objStockChart.BlockRequest() 로 요청한 후 이 메소드로 통신상태 검사해야함
        :return: None
        """
        rqStatus = self.objStockChart.GetDibStatus()
        rqRet = self.objStockChart.GetDibMsg1()
        if rqStatus == 0:
            pass
            # print("통신상태 정상[{}]{}".format(rqStatus, rqRet), end=' ')
        else:
            print("통신상태 오류[{}]{} 종료합니다..".format(rqStatus, rqRet))
            exit()

    async def apply_delay(self):
        current_time = datetime.now().time()
        if (current_time >= datetime.strptime("09:00", "%H:%M").time() and current_time <= datetime.strptime("09:10", "%H:%M").time()) or (current_time >= datetime.strptime("15:20", "%H:%M").time() and current_time <= datetime.strptime("15:30", "%H:%M").time()):
            await asyncio.sleep(0.7)  # 바쁜 시간대 딜레이
        else:
            await asyncio.sleep(0.5)  # 일반 시간대 딜레이


    # 차트 요청 - 최근일 부터 개수 기준
    async def RequestDWM(self, code, dwm, count, caller: 'MainWindow', from_date=0):
        """
        :param code: 종목코드
        :param dwm: 'D':일봉, 'W':주봉, 'M':월봉
        :param count: 요청할 데이터 개수
        :return: None
        """
        self.objStockChart.SetInputValue(0, code)  # 종목코드
        self.objStockChart.SetInputValue(1, ord('2'))  # 개수로 받기
        self.objStockChart.SetInputValue(4, count)  # 최근 count개

        # 요청항목
        self.objStockChart.SetInputValue(5, [0, # 날짜 (ulong)
                                            2, # 시가
                                            3, # 고가
                                            4, # 저가
                                            5, # 종가
                                            8, # 거래량
                                            9, # 거래대금(ulonglong)
                                            13, # 시가총액(ulonglong)
                                            ])
        # 요청한 항목들을 튜플로 만들어 사용
        rq_column = ('date', 'open', 'high', 'low', 'close', 'volume', 'value', 'marketC')

        self.objStockChart.SetInputValue(6, ord(dwm))  # '차트 주기 - 일/주/월
        self.objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용

        rcv_data = {}
        for col in rq_column:
            rcv_data[col] = []

        rcv_count = 0
        while count > rcv_count:
            self.objStockChart.BlockRequest()  # 요청! 후 응답 대기
            self._check_rq_status()  # 통신상태 검사
            await self.apply_delay() # 시간당 RQ 제한으로 인해 장애가 발생하지 않도록 딜레이를 줌
            # time.sleep(0.25)

            rcv_batch_len = self.objStockChart.GetHeaderValue(3)  # 받아온 데이터 개수
            rcv_batch_len = min(rcv_batch_len, count - rcv_count)  # 정확히 count 개수만큼 받기 위함
            for i in range(rcv_batch_len):
                for col_idx, col in enumerate(rq_column):
                    rcv_data[col].append(self.objStockChart.GetDataValue(col_idx, i))

            if len(rcv_data['date']) == 0:  # 데이터가 없는 경우
                # print(code, '데이터 없음')
                return False

            # rcv_batch_len 만큼 받은 데이터의 가장 오래된 date
            rcv_oldest_date = rcv_data['date'][-1]
            rcv_count += rcv_batch_len
            caller.return_status_msg = '{} / {}'.format(rcv_count, count)
            
            # 서버가 가진 모든 데이터를 요청한 경우 break.
            # self.objStockChart.Continue 는 개수로 요청한 경우
            # count만큼 이미 다 받았더라도 계속 1의 값을 가지고 있어서
            # while 조건문에서 count > rcv_count를 체크해줘야 함.
            if not self.objStockChart.Continue:
                break
            if rcv_oldest_date < from_date:
                break
            
        caller.rcv_data = rcv_data  # 받은 데이터를 caller의 멤버에 저장
        return True

    # 차트 요청 - 분간, 틱 차트
    async def RequestMT(self, code, dwm, tick_range, count, caller: 'MainWindow', from_date=0):
        """
        :param code: 종목 코드
        :param dwm: 'm':분봉, 'T':틱봉
        :param tick_range: 1분봉 or 5분봉, ...
        :param count: 요청할 데이터 개수
        :param caller: 이 메소드 호출한 인스턴스. 결과 데이터를 caller의 멤버로 전달하기 위함
        :return:
        """
        self.objStockChart.SetInputValue(0, code)  # 종목코드
        self.objStockChart.SetInputValue(1, ord('2'))  # 개수로 받기
        self.objStockChart.SetInputValue(4, count)  # 조회 개수
        # 요청항목
        self.objStockChart.SetInputValue(5, [0, # 날짜(ulong)
                                            1, # 시간(long) - hhmm
                                            2, # 시가(long or float)
                                            3, # 고가(long or float)
                                            4, # 저가(long or float)
                                            5, # 종가(long or float)
                                            8, # 거래량
                                            9 # 거래대금
                                            ])
        # 요청한 항목들을 튜플로 만들어 사용
        rq_column = ('date', 'time', 'open', 'high', 'low', 'close', 'volume', 'value')

        self.objStockChart.SetInputValue(6, ord(dwm))  # '차트 주기 - 분/틱
        self.objStockChart.SetInputValue(7, tick_range)  # 분틱차트 주기
        self.objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용

        rcv_data = {}
        for col in rq_column:
            rcv_data[col] = []

        rcv_count = 0
        while count > rcv_count:
            self.objStockChart.BlockRequest()  # 요청! 후 응답 대기
            self._check_rq_status()  # 통신상태 검사
            await self.apply_delay() # 시간당 RQ 제한으로 인해 장애가 발생하지 않도록 딜레이를 줌
            # time.sleep(0.25)  

            rcv_batch_len = self.objStockChart.GetHeaderValue(3)  # 받아온 데이터 개수
            rcv_batch_len = min(rcv_batch_len, count - rcv_count)  # 정확히 count 개수만큼 받기 위함
            for i in range(rcv_batch_len):
                for col_idx, col in enumerate(rq_column):
                    rcv_data[col].append(self.objStockChart.GetDataValue(col_idx, i))

            if len(rcv_data['date']) == 0:  # 데이터가 없는 경우
                # print(code, '데이터 없음')
                return False

            # len 만큼 받은 데이터의 가장 오래된 date
            rcv_oldest_date = int('{}{:04}'.format(rcv_data['date'][-1], rcv_data['time'][-1]))
            rcv_count += rcv_batch_len
            caller.return_status_msg = '{} / {}(maximum)'.format(rcv_count, count)

            # 서버가 가진 모든 데이터를 요청한 경우 break.
            # self.objStockChart.Continue 는 개수로 요청한 경우
            # count만큼 이미 다 받았더라도 계속 1의 값을 가지고 있어서
            # while 조건문에서 count > rcv_count를 체크해줘야 함.
            if not self.objStockChart.Continue:
                break
            if rcv_oldest_date < from_date:
                break

        # 분봉의 경우 날짜와 시간을 하나의 문자열로 합친 후 int로 변환
        rcv_data['date'] = list(map(lambda x, y: int('{}{:04}'.format(x, y)),
                 rcv_data['date'], rcv_data['time']))
        del rcv_data['time']
        caller.rcv_data = rcv_data  # 받은 데이터를 caller의 멤버에 저장
        return True
    
# 종목코드 관리하는 클래스
class CpCodeMgr:
    def __init__(self):
        self.objCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")

    # 마켓에 해당하는 종목코드 리스트 반환하는 메소드
    def get_code_list(self, market):
        """
        :param market: 1:코스피, 2:코스닥, ...
        :return: market에 해당하는 코드 list
        """
        code_list = self.objCodeMgr.GetStockListByMarket(market)
        return code_list

    # 부구분코드를 반환하는 메소드
    def get_section_code(self, code):
        section_code = self.objCodeMgr.GetStockSectionKind(code)
        return section_code

    # 종목 코드를 받아 종목명을 반환하는 메소드
    def get_code_name(self, code):
        code_name = self.objCodeMgr.CodeToName(code)
        return code_name

    # 종목 코드를 받아 소속부를 반환하는 메소드
    def get_market_kind(self, code):
        market_kind = self.objCodeMgr.GetStockMarketKind(code)
        return market_kind

    # 종목 코드를 받아 주식상태를 반환하는 메소드 typedefenum({CPC_STOCK_STATUS_NORMAL= 0, CPC_STOCK_STATUS_STOP= 1, CPC_STOCK_STATUS_BREAK= 2} }CPE_SUPERVISION_KIND;
    def get_code_status(self, code):
        code_status = self.objCodeMgr.GetStockStatusKind(code)
        return code_status

class CpStockUniWeek:
    def __init__(self):
        self.objStockUniWeek = win32com.client.Dispatch("CpSysDib.StockUniWeek")

    def _check_rq_status(self):
        rqStatus = self.objStockUniWeek.GetDibStatus()
        rqRet = self.objStockUniWeek.GetDibMsg1()
        if rqStatus != 0:
            print(f"통신상태 오류[{rqStatus}]{rqRet}")
            raise ConnectionError(f"통신상태 오류[{rqStatus}]{rqRet}")
            
    async def apply_delay(self):
        current_time = datetime.now().time()
        if (current_time >= datetime.strptime("09:00", "%H:%M").time() and current_time <= datetime.strptime("09:10", "%H:%M").time()) or \
        (current_time >= datetime.strptime("15:20", "%H:%M").time() and current_time <= datetime.strptime("15:30", "%H:%M").time()):
            await asyncio.sleep(0.7)
        else:
            await asyncio.sleep(0.5)

    async def request_stock_data(self, code, count, caller=None, from_date=0):
        self.objStockUniWeek.SetInputValue(0, code)

        rq_column = ('date', 'open', 'high', 'low', 'close','diff', 'diff_rate')
        rcv_data2 = {}
        rcv_data2 = {col: [] for col in rq_column}

        rcv_count = 0
        while count > rcv_count:
            self.objStockUniWeek.BlockRequest()
            self._check_rq_status()
            await self.apply_delay()

            rcv_batch_len = self.objStockUniWeek.GetHeaderValue(1)
            rcv_batch_len = min(rcv_batch_len, count - rcv_count)

            for i in range(rcv_batch_len):
                item_date = self.objStockUniWeek.GetDataValue(0, i)
                if item_date < from_date:
                    break

                for col_idx, col in enumerate(rq_column):
                    rcv_data2[col].append(self.objStockUniWeek.GetDataValue(col_idx, i))

            if len(rcv_data2['date']) == 0:
                # print(code, '데이터 없음')
                return False

            rcv_oldest_date = rcv_data2['date'][-1]
            rcv_count += rcv_batch_len
            if caller:
                caller.return_status_msg = '{} / {}'.format(rcv_count, count)

            if not self.objStockUniWeek.Continue:
                break
            if rcv_oldest_date < from_date:
                break

        if caller:
            caller.rcv_data2 = rcv_data2
        await self.apply_delay()
        return True

