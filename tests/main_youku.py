import time
import requests
import json
import re

from movie_repository.entity import MovieEntity


def safe_float_conversion(value, default=-1.0):
    """尝试将给定的值转换为浮点数，如果失败则返回默认值"""
    try:
        return float(value)
    except ValueError:
        return default


class YouKu:
    pagesize: int = 60
    platform: str = 'youku'
    base_url: str = 'https://www.youku.com/category/data'
    params: dict[str, str] = {
        "session": '{"subIndex":24,"trackInfo":{"parentdrawerid":"34441"},"spmA":"a2h05","level":2,"spmC":"drawer2",'
                   '"spmB":"8165803_SHAIXUAN_ALL","index":1,"pageName":"page_channelmain_SHAIXUAN_ALL",'
                   '"scene":"search_component_paging","scmB":"manual","path":"EP557352,EP557350,EP557347,EP557346,'
                   'EP557345","scmA":"20140719","scmC":"34441","from":"SHAIXUAN","id":227939,"category":"电影"}',
        "params": '{"type":"电影"}',
        "pageNo": "1"  # 默认从1开始
    }
    youku_headers = {
        "Referer": "https://www.youku.com/category/show/type_%E7%94%B5%E5%BD%B1.html",
    }

    @staticmethod
    def get_movie_info(obj_data: any):
        response = requests.get('https:' + obj_data["videoLink"])
        j = re.findall(r"__INITIAL_DATA__ =(.+)?;<", response.text)[0]
        json_object = json.loads(j)
        root_data = json_object["data"]["data"]
        root_nodes = json_object["data"]["model"]["detail"]["data"]["nodes"]

        item = {
            'intro': '',
            'release_date': '',
            'cover_url': '',
            'score': -1.0,
            'movie_type': [],
            'directors': [],
            'actors': []
        }
        if root_data["type"] == 10001:
            item['release_date'] = root_data["data"]["extra"]["showReleaseTime"]  # 发布日期
        for node in root_nodes:
            if not node["type"] == 10001:
                continue
            sub_nodes = node["nodes"]
            for sub_node in sub_nodes:
                if not sub_node["type"] == 20009:
                    continue
                child_nodes = sub_node["nodes"]
                for child_node in child_nodes:
                    if child_node["type"] == 20010:
                        data = child_node["data"]
                        item['cover_url'] = data["showImgV"]
                        item['score'] = safe_float_conversion(data.get('score', ''))
                        item['movie_type'] = data["introSubTitle"]
                        item['intro'] = data["desc"]
                    elif child_node["type"] == 10011:
                        data = child_node["data"]
                        if "导演" in data["subtitle"]:
                            item['directors'].append(data["title"])
                        else:
                            item['actors'].append(data["title"])
        time.sleep(1)
        return MovieEntity(
            title=obj_data["title"],
            cover_url=item['cover_url'],
            intro=item['intro'],
            score=item['score'],
            directors=item['directors'],
            actors=item['actors'],
            release_date=item['release_date'],
            movie_type=item['movie_type'],
            metadata={
                'source': 'YouKu',
                'summary': obj_data
            },
        )

    @staticmethod
    def get_movies(page: int = 1) -> list[MovieEntity]:
        YouKu.params['pageNo'] = str(page)
        response = requests.get(f"{YouKu.base_url}", params=YouKu.params, headers=YouKu.youku_headers)
        json_object = json.loads(response.text)
        result: list[MovieEntity] = []
        results = json_object["data"]["filterData"]["listData"]
        for data in results:
            result.append(YouKu.get_movie_info(data))
        return result


# # 假设 get_movies 已经定义并且准备好使用
# async def main():
#     movies = await YouKu.get_movies()  # 调用异步函数并传递参数
#     print(movies[0])  # 打印结果

# 运行主函数
if __name__ == "__main__":
    # asyncio.run(main())
    print(YouKu.get_movies()[0])
