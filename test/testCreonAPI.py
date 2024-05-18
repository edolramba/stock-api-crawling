import sys
sys.path.append("C:\\Dev\\stock-api-crawling")  # creonAPI 모듈이 있는 경로를 추가
from api.creonAPI import CpStockUniWeek  # creonAPI 모듈에서 CpStockUniWeek 클래스를 가져옴

def test_stock_uni_week():
    # 인스턴스 생성
    cp_stock_uni_week = CpStockUniWeek()
    
    # 종목 코드와 데이터 수를 지정하여 시간외 단일가 데이터 요청
    # 예: 삼성전자의 최근 10일간 데이터
    code = "005930"  # 삼성전자의 종목 코드
    days_count = 10  # 데이터 요청 일수
    result = cp_stock_uni_week.get_data(code, days_count)

    if result:
        print("데이터 수신 성공:")
        for data in result:
            print(data)
    else:
        print("데이터 수신 실패")

if __name__ == "__main__":
    test_stock_uni_week()
