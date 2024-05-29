from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['movie_repository']
collection = db['test_collection']

obj1 = {'_id': 'unique_id', 'data': 'obj1', 'platform_detail': {"source": "mgtv", "title": "唐人街探案3", "cover_url": "https://1img.hitv.com/preview/internettv/sp_images/ott/2020/3/14/dianying/333023/20200314130521857-new.jpg", "create_time": "2024-05-29 00:19:22.694", "directors": [], "actors": ["王宝强", "刘昊然", "妻夫木聪", "托尼·贾", "长泽雅美", "张子枫", "铃木保奈美", "染谷将太", "浅野忠信", "三浦友和", "尚语贤", "肖央", "邱泽", "张钧甯", "马伯骞", "程潇"], "release_date": "2020", "description": "继曼谷、纽约之后，东京再出大案。唐人街神探唐仁（王宝强饰）、秦风（刘昊然饰）受侦探野田昊（妻夫木聪饰）的邀请前往破案。“CRIMASTER世界侦探排行榜”中的侦探们闻讯后也齐聚东京，加入挑战。而排名第一Q的现身，让这个大案更加扑朔迷离，一场亚洲最强神探之间的较量即将爆笑展开……", "score": 0, "movie_type": ["喜剧", "悬疑", "动作"], "metadata": {"clip_id": "333023", "views": "979465", "update_time": "2023-06-09 11:05:18"}}}

obj2 = {'_id': 'unique_id', 'data': 'obj2', 'platform_detail': {"source": "iqiyi", "title": "唐人街探案3", "cover_url": "https://pic6.iqiyipic.com/image/20231109/ee/15/v_115684213_m_601_m23.webp", "create_time": "2024-05-29 00:19:45.670", "directors": [], "actors": ["王宝强", "刘昊然", "妻夫木聪", "张子枫", "托尼·贾", "长泽雅美", "染谷将太", "铃木保奈美"], "release_date": "2021-2-12", "description": "继曼谷、纽约之后，东京再出大案。唐人街神探唐仁（王宝强饰）、秦风（刘昊然饰）受侦探野田昊（妻夫木聪饰）的邀请前往破案。“CRIMASTER世界侦探排行榜”中的侦探们闻讯后也齐聚东京，加入挑战，而排名第一Q的现身，让这个大案更加扑朔迷离，一场亚洲最强神探之间的较量即将爆笑展开……", "score": 7.3, "movie_type": ["悬疑", "喜剧", "犯罪", "动作", "剧情", "推理", "搞笑", "内地", "普通话", "院线"], "metadata": {"batch": "7", "batch_order": "1", "entity_id": "1008748400", "album_id": "1008748400", "tv_id": "1008748400"}}}

obj3 = {'_id': 'unique_id', 'data': 'obj3', 'platform_detail': {"source": "iqiyi", "title": "唐人街探案3", "cover_url": "https://pic6.iqiyipic.com/image/20231109/ee/15/v_115684213_m_601_m23.webp", "create_time": "2024-05-29 00:19:56.986", "directors": [], "actors": ["王宝强", "刘昊然", "妻夫木聪", "张子枫", "托尼·贾", "长泽雅美", "染谷将太", "铃木保奈美"], "release_date": "2021-2-12", "description": "继曼谷、纽约之后，东京再出大案。唐人街神探唐仁（王宝强饰）、秦风（刘昊然饰）受侦探野田昊（妻夫木聪饰）的邀请前往破案。“CRIMASTER世界侦探排行榜”中的侦探们闻讯后也齐聚东京，加入挑战，而排名第一Q的现身，让这个大案更加扑朔迷离，一场亚洲最强神探之间的较量即将爆笑展开……", "score": 7.3, "movie_type": ["悬疑", "喜剧", "犯罪", "动作", "剧情", "推理", "搞笑", "内地", "普通话", "院线"], "metadata": {"batch": "10", "batch_order": "54", "entity_id": "1008748400", "album_id": "1008748400", "tv_id": "1008748400"}}}

obj4 = {'_id': 'unique_id', 'data': 'obj4', 'platform_detail': {"source": "iqiyi", "title": "唐人街探案3", "cover_url": "https://pic6.iqiyipic.com/image/20231109/ee/15/v_115684213_m_601_m23.webp", "create_time": "2024-05-29 00:20:00.729", "directors": [], "actors": ["王宝强", "刘昊然", "妻夫木聪", "张子枫", "托尼·贾", "长泽雅美", "染谷将太", "铃木保奈美"], "release_date": "2021-2-12", "description": "继曼谷、纽约之后，东京再出大案。唐人街神探唐仁（王宝强饰）、秦风（刘昊然饰）受侦探野田昊（妻夫木聪饰）的邀请前往破案。“CRIMASTER世界侦探排行榜”中的侦探们闻讯后也齐聚东京，加入挑战，而排名第一Q的现身，让这个大案更加扑朔迷离，一场亚洲最强神探之间的较量即将爆笑展开……", "score": 7.3, "movie_type": ["悬疑", "喜剧", "犯罪", "动作", "剧情", "推理", "搞笑", "内地", "普通话", "院线"], "metadata": {"batch": "11", "batch_order": "5", "entity_id": "1008748400", "album_id": "1008748400", "tv_id": "1008748400"}}}


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
