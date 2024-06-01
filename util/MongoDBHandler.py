from common.importConfig import *
import pymongo
from pymongo import MongoClient
from pymongo.cursor import CursorType
import datetime
from datetime import date

class MongoDBHandler:
    
    def __init__(self):
        importConf = importConfig()
        host = importConf.select_section("MONGODB")["host"]
        port = importConf.select_section("MONGODB")["port"]
        self._client = MongoClient(host, int(port))
        self._session = None

    def start_session(self):
        if self._session is None:
            self._session = self._client.start_session()
        return self._session

    def end_session(self):
        if self._session is not None:
            self._session.end_session()
            self._session = None

    def ensure_unique_index(self, db_name, collection_name, field_name):
        self.validate_params(db_name, collection_name)
        current_indexes = self._client[db_name][collection_name].index_information()
        if field_name not in current_indexes:
            self._client[db_name][collection_name].create_index([(field_name, pymongo.ASCENDING)], unique=True)

    def validate_params(self, db_name, collection_name):
        if not db_name or not collection_name:
            raise Exception("Database name and collection name must be provided.")
    
    def insert_item(self, data, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if not isinstance(data, dict):
            raise Exception("data type should be dict")
        return self._client[db_name][collection_name].insert_one(data, session=session).inserted_id

    def insert_items(self, datas, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if not isinstance(datas, list):
            raise Exception("datas type should be list")
        return self._client[db_name][collection_name].insert_many(datas, session=session).inserted_ids

    def find_item(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        query_options = {"_id": False}
        if projection:
            query_options.update(projection)
        cursor = self._client[db_name][collection_name].find(condition, query_options, session=session)
        if sort:
            cursor.sort(sort)
        cursor.limit(1)
        try:
            return next(cursor, None)
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def find_items(self, condition=None, db_name=None, collection_name=None, sort=None, projection=None, limit=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        find_options = {}
        
        if projection:
            find_options['projection'] = projection
        
        cursor = self._client[db_name][collection_name].find(condition, **find_options, session=session)
        
        if sort:
            cursor = cursor.sort(sort)
        if limit:
            cursor = cursor.limit(limit)

        return list(cursor)

    def find_items_distinct(self, condition=None, db_name=None, collection_name=None, distinct_col=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find(condition, {"_id": False}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST, session=session).distinct(distinct_col)

    def find_items_id(self, condition=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find(condition, {"_id": True}, no_cursor_timeout=True, cursor_type=CursorType.EXHAUST, session=session)

    def find_item_id(self, condition=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        condition = condition if isinstance(condition, dict) else {}
        return self._client[db_name][collection_name].find_one(condition, {"_id": True}, session=session)

    def delete_items(self, condition=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict):
            raise Exception("Condition must be provided as dict")
        return self._client[db_name][collection_name].delete_many(condition, session=session)

    def update_items(self, condition=None, update_value=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_many(filter=condition, update=update_value, session=session)

    def update_item(self, condition=None, update_value=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_one(filter=condition, update=update_value, session=session)
    
    def aggregate(self, pipeline=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if pipeline is None or not isinstance(pipeline, list):
            raise Exception("Pipeline must be provided as a list")
        return self._client[db_name][collection_name].aggregate(pipeline, session=session)

    def text_search(self, text=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if text is None or not isinstance(text, str):
            raise Exception("Text must be provided")
        return self._client[db_name][collection_name].find({"$text": {"$search": text}}, session=session)

    def upsert_item(self, condition=None, update_value=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_one(filter=condition, update=update_value, upsert=True, session=session)

    def upsert_items(self, condition=None, update_value=None, db_name=None, collection_name=None, session=None):
        self.validate_params(db_name, collection_name)
        if condition is None or not isinstance(condition, dict) or update_value is None:
            raise Exception("Both condition and update value must be provided")
        return self._client[db_name][collection_name].update_many(filter=condition, update=update_value, upsert=True, session=session)

    def validate_params(self, db_name, collection_name=None):
        if not db_name:
            raise Exception("Database name must be provided.")
        if collection_name is not None and not collection_name:
            raise Exception("Collection name must be provided when specified.")
    
    def list_collections(self, db_name):
        db = self._client[db_name]
        return db.list_collection_names()
    
    def check_database_exists(self, db_name):
        self.validate_params(db_name)
        return len(self._client[db_name].list_collection_names()) > 0
    
    def delete_column(self, db_name, column_name):
        self.validate_params(db_name)
        collection_names = self.list_collections(db_name)
        for collection_name in collection_names:
            self._client[db_name][collection_name].update_many({}, {"$unset": {column_name: ""}})
