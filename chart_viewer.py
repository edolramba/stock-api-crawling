import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QComboBox, QLineEdit, QPushButton, 
                           QLabel, QMessageBox, QCompleter)
from PyQt5.QtCore import Qt
import pandas as pd
import mplfinance as mpf
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from util.MongoDBHandler import MongoDBHandler
from datetime import datetime, timedelta
import logging
import matplotlib
import matplotlib.font_manager as fm

# 파일 상단에 로거 설정 추가
logger = logging.getLogger('chart_viewer')
if not logger.hasHandlers():
    handler = logging.FileHandler('chart_viewer.log', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

font_path = 'fonts/NANUMGOTHIC.TTF'
fontprop = fm.FontProperties(fname=font_path)
matplotlib.rc('font', family='DejaVu Sans')
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.rcParams['mathtext.fontset'] = 'dejavusans'
matplotlib.rcParams['mathtext.rm'] = fontprop.get_name()
matplotlib.rcParams['mathtext.it'] = fontprop.get_name()
matplotlib.rcParams['mathtext.bf'] = fontprop.get_name()

class ChartViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_handler = MongoDBHandler()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('주식 차트 뷰어')
        self.setGeometry(100, 100, 1200, 800)
        
        # 메인 위젯과 레이아웃
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # 검색 영역
        search_layout = QHBoxLayout()
        
        # 종목코드/종목명 입력
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('종목코드 또는 종목명 입력')
        
        # 자동완성 설정
        self.setup_autocomplete()
        
        search_layout.addWidget(self.search_input)
        
        # 검색 버튼
        search_btn = QPushButton('검색')
        search_btn.clicked.connect(self.search_stock)
        search_layout.addWidget(search_btn)
        
        # 기간 선택
        self.period_combo = QComboBox()
        self.period_combo.addItems(['1주일', '1개월', '3개월', '6개월', '1년'])
        search_layout.addWidget(QLabel('기간:'))
        search_layout.addWidget(self.period_combo)
        
        # 차트 타입 선택
        self.chart_type_combo = QComboBox()
        self.chart_type_combo.addItems(['일봉', '분봉'])
        search_layout.addWidget(QLabel('차트 타입:'))
        search_layout.addWidget(self.chart_type_combo)
        
        # 새로고침 버튼
        refresh_btn = QPushButton('새로고침')
        refresh_btn.clicked.connect(self.refresh_chart)
        search_layout.addWidget(refresh_btn)
        
        layout.addLayout(search_layout)
        
        # 차트 영역
        self.figure = Figure(figsize=(12, 8))
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)
        
        # 상태바
        self.statusBar().showMessage('준비')
        
    def setup_autocomplete(self):
        # MongoDB에서 종목 리스트 가져오기
        stocks = list(self.db_handler._client['sp_common']['sp_all_code_name'].find(
            {},
            {'stock_code': 1, 'stock_name': 1, '_id': 0}
        ))
        
        # 종목명과 종목코드를 조합한 리스트 생성
        self.stock_list = [f"{stock['stock_name']} ({stock['stock_code']})" for stock in stocks]
        
        # QCompleter 설정
        completer = QCompleter(self.stock_list)
        completer.setCaseSensitivity(Qt.CaseInsensitive)  # 대소문자 구분 없음
        completer.setFilterMode(Qt.MatchContains)  # 부분 일치 검색
        completer.setMaxVisibleItems(10)  # 최대 표시 항목 수
        
        self.search_input.setCompleter(completer)
        
    def search_stock(self):
        search_text = self.search_input.text().strip()
        if not search_text:
            QMessageBox.warning(self, '경고', '종목코드 또는 종목명을 입력하세요.')
            return
            
        # 입력된 텍스트에서 종목코드 추출
        if '(' in search_text and ')' in search_text:
            stock_code = search_text.split('(')[-1].split(')')[0]
            stock_info = self.db_handler.find_item(
                {'stock_code': stock_code},
                db_name='sp_common',
                collection_name='sp_all_code_name'
            )
        else:
            # 종목코드로 검색
            stock_info = self.db_handler.find_item(
                {'stock_code': search_text},
                db_name='sp_common',
                collection_name='sp_all_code_name'
            )
            
            # 종목명으로 검색 (부분 일치)
            if not stock_info:
                stock_info = self.db_handler.find_item(
                    {'stock_name': {'$regex': search_text, '$options': 'i'}},
                    db_name='sp_common',
                    collection_name='sp_all_code_name'
                )
            
        if not stock_info:
            QMessageBox.warning(self, '경고', '종목을 찾을 수 없습니다.')
            return
            
        self.current_stock_code = stock_info['stock_code']
        self.current_stock_name = stock_info['stock_name']
        self.refresh_chart()
        
    def refresh_chart(self):
        if not hasattr(self, 'current_stock_code'):
            return
            
        # 기간 설정
        period = self.period_combo.currentText()
        end_date = datetime.now()
        if period == '1주일':
            start_date = end_date - timedelta(days=7)
        elif period == '1개월':
            start_date = end_date - timedelta(days=30)
        elif period == '3개월':
            start_date = end_date - timedelta(days=90)
        elif period == '6개월':
            start_date = end_date - timedelta(days=180)
        else:  # 1년
            start_date = end_date - timedelta(days=365)
            
        # 차트 타입에 따라 DB 선택
        db_name = 'sp_day' if self.chart_type_combo.currentText() == '일봉' else 'sp_1min'
        
        # 컬렉션명은 sp_all_code_name의 stock_code를 그대로 사용
        collection_code = self.current_stock_code
        
        # 날짜를 YYYYMMDD(일봉) 또는 YYYYMMDDHHMM(분봉) 형식의 정수로 변환
        if db_name == 'sp_day':
            start_date_int = int(start_date.strftime('%Y%m%d'))
            end_date_int = int(end_date.strftime('%Y%m%d'))
        else:  # 분봉
            start_date_int = int(start_date.strftime('%Y%m%d') + '0000')
            end_date_int = int(end_date.strftime('%Y%m%d') + '2359')
        
        # 데이터 조회
        query = {
            'date': {
                '$gte': start_date_int,
                '$lte': end_date_int
            }
        }
        
        try:
            cursor = self.db_handler._client[db_name][collection_code].find(
                query,
                sort=[('date', 1)]
            )
            
            data = list(cursor)
            if not data:
                logger.warning(f"데이터 없음: 종목명={self.current_stock_name}, 종목코드={collection_code}, DB={db_name}, 쿼리={query}")
                QMessageBox.warning(self, '경고', '데이터가 없습니다.')
                return
                
            # DataFrame 생성
            df = pd.DataFrame(data)
            
            # date 컬럼을 datetime으로 변환
            if db_name == 'sp_day':
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
            else:
                df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d%H%M')
            df.set_index('date', inplace=True)
            
            # 기존 FigureCanvas 제거
            layout = self.centralWidget().layout()
            layout.removeWidget(self.canvas)
            self.canvas.setParent(None)
            
            # mplfinance로 Figure 생성 (returnfig=True 사용)
            fig, _ = mpf.plot(
                df,
                type='candle',
                style='charles',
                title=dict(title=f'{self.current_stock_name} ({self.current_stock_code})', fontproperties=fontprop),
                ylabel='가격',
                volume=True,
                show_nontrading=False,
                returnfig=True,
                figsize=(12, 8)
            )
            
            # 새 FigureCanvas로 교체
            from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
            self.canvas = FigureCanvas(fig)
            layout.addWidget(self.canvas)
            self.statusBar().showMessage(f'{self.current_stock_name} ({self.current_stock_code}) 차트 업데이트 완료')
            
        except Exception as e:
            logger.error(f"차트 데이터 조회 중 오류: 종목명={self.current_stock_name}, 종목코드={collection_code}, DB={db_name}, 쿼리={query}, 오류={str(e)}")
            QMessageBox.warning(self, '오류', f'차트 데이터 조회 중 오류가 발생했습니다: {str(e)}')
            return

def main():
    app = QApplication(sys.argv)
    viewer = ChartViewer()
    viewer.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main() 