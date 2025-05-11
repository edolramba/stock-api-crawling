from pymongo import MongoClient
from bson import Int64

client = MongoClient('mongodb://localhost:27017/')
db = client['sp_1min']
col = db['A000020']

# 1. 테스트용 데이터
test_doc = {
    'date': Int64(202505091530),
    'open': 6220,
    'high': 6220,
    'low': 6220,
    'close': 6220,
    'volume': 2413,
    'value': 15010000
}

# 2. 저장(업서트)
result = col.update_one({'date': test_doc['date']}, {'$set': test_doc}, upsert=True)
print('업서트 결과:', result.raw_result)

# 3. 저장 후 확인
saved = col.find_one({'date': test_doc['date']})
print('저장된 문서:', saved)

# 4. 삭제
delete_result = col.delete_one({'date': test_doc['date']})
print('삭제 결과:', delete_result.raw_result)

# 5. 삭제 후 확인
deleted = col.find_one({'date': test_doc['date']})
print('삭제 후 문서:', deleted)