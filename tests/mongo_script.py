import json
from pymongo import MongoClient

# 连接到 MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["movie_repository"]
collection = db["movies"]

# 遍历集合中的每个文档
for doc in collection.find():
    if 'platform_detail' in doc and isinstance(doc['platform_detail'], str):
        print(doc['platform_detail'])
        platform_detail_array = json.loads(doc['platform_detail'])
        try:
            # 尝试将字符串解析为 JSON 数组
            # 更新文档中的 platform_detail 字段为数组
            collection.update_one(
                {'_id': doc['_id']},
                {'$set': {'platform_detail': platform_detail_array}}
            )
            print(f"Updated document with _id: {doc['_id']}")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON for document with _id: {doc['_id']}, error: {e}")

print("Completed updating documents.")
