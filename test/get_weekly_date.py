import datetime

def get_weekly_date(latest_date):
    latest_date_str = str(latest_date)
    date_object = datetime.datetime.strptime(latest_date_str, '%Y%m%d')
    
    # 해당 월의 첫 날
    first_day_of_month = datetime.datetime(date_object.year, date_object.month, 1)
    
    # 해당 월의 첫 번째 일요일 찾기
    first_sunday = first_day_of_month + datetime.timedelta(days=(6 - first_day_of_month.weekday()))
    
    # 주차 계산 (첫 일요일 이전 날짜들은 첫 주차로 계산)
    if date_object < first_sunday:
        week_of_month = 1
    else:
        week_of_month = ((date_object - first_sunday).days // 7) + 1
    
    # 날짜 형식 YYYYMMWW (WW는 주차 * 10)
    formatted_date = f"{date_object.year}{date_object.month:02}{week_of_month * 10}"
    
    return formatted_date

# 예제로 날짜 확인
print(get_weekly_date(20230901))  # 2023년 10월 1일
print(get_weekly_date(20230930))  # 2023년 10월 31일
print(get_weekly_date(20231001))  # 2023년 10월 1일
print(get_weekly_date(20231031))  # 2023년 10월 31일
print(get_weekly_date(20231101))  # 2023년 11월 1일
print(get_weekly_date(20231130))  # 2023년 11월 30일
