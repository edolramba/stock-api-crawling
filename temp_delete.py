from pymongo import MongoClient

# MongoDB 연결 설정
client = MongoClient("mongodb://localhost:27017")  # 필요에 따라 주소 변경
db = client["sp_day"]  # sp_day 데이터베이스 선택

# 모든 컬렉션 목록 가져오기
collections = db.list_collection_names()

# 각 컬렉션에서 'date' 필드가 '20250312'인 문서 삭제
for collection_name in collections:
    collection = db[collection_name]
    result = collection.delete_many({"date": 20250312})
    print(f"컬렉션 {collection_name}: {result.deleted_count}개 문서 삭제됨.")

# MongoDB 연결 종료
client.close()
