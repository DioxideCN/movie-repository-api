from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['movie_repository']
collection = db['test_collection']

obj1 = {'_id': 'unique_id', 'data': 'obj1', 'platform_detail': {'source': 'A', 'order': 'order1'}}
obj2 = {'_id': 'unique_id', 'data': 'obj2', 'platform_detail': {'source': 'A', 'order': 'order2'}}
obj3 = {'_id': 'unique_id', 'data': 'obj3', 'platform_detail': {'source': 'B', 'order': 'order3'}}
obj4 = {'_id': 'unique_id_2', 'data': 'obj4', 'platform_detail': {'source': 'B', 'order': 'order4'}}


def update_source(movie_dict: dict[str, any]):
    platform_data_item = movie_dict.pop('platform_detail')
    collection.update_one(
        {"_id": movie_dict['_id']},
        [{
            "$set": {
                "data": movie_dict['data'],  # 更新顶级data字段
                "platform_data": {
                    "$cond": {
                        "if": {"$isArray": "$platform_data"},
                        "then": {
                            "$function": {
                                "body": """function(platform_data, new_data) { let index = 
                                        platform_data.findIndex(item => item.source === new_data.source); if (index 
                                        !== -1) { platform_data[index] = new_data; } else { platform_data.push(
                                        new_data); } return platform_data; }""",
                                "args": ["$platform_data", platform_data_item],
                                "lang": "js"
                            }
                        },
                        "else": [platform_data_item]  # 如果platform_data不存在，创建新数组
                    }
                }
            }
        }],
        upsert=True
    )


update_source(obj1)
update_source(obj2)
update_source(obj3)
update_source(obj4)
