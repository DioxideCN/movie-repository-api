import json
import time
import re

import requests
from requests import Response
from lxml import etree

from movie_repository.util.logger import logger
from movie_repository.entity import MovieEntity
from .initializer_config import write_in

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}


class Bilibili:
    @staticmethod
    def resolve_ep_id(link: str) -> str:
        return link.split('/bangumi/play/')[-1].split('?')[0]

    @staticmethod
    def fetch_actors(ep_id: str) -> [str]:
        response: Response = requests.get(f'https://www.bilibili.com/bangumi/play/ep{ep_id}', headers=headers)
        response.encoding = 'utf-8'
        html: any = etree.HTML(response.text)
        div_texts: [str] = html.xpath("//div[contains(text(), '出演演员')]//text()")
        return ''.join(t.strip() for t in div_texts).removeprefix('出演演员：').split('\n')

    @staticmethod
    def handle_data(raw_film_data: any) -> [MovieEntity]:
        movie_list: [MovieEntity] = []
        if 'data' in raw_film_data:
            data_film_data: any = raw_film_data['data']
            if 'list' in data_film_data:
                film_data: [any] = data_film_data['list']
                for item in film_data:  # 遍历JSON结果转换为MovieEntity对象
                    eq_id: str = str(item['first_ep']['ep_id'])
                    movie_list.append(MovieEntity(  # 添加数据到结果集
                        title=item['title'],
                        cover_url=item['cover'],
                        intro=item.get('subTitle', ''),
                        score=safe_float_conversion(item.get('score', ''), -1.0),
                        actors=Bilibili.fetch_actors(eq_id),
                        release_date=str(item['index_show']).removesuffix('上映'),
                        metadata={
                            'source': 'Bilibili',
                            'id': eq_id,
                            'order': str(item['order']).removesuffix('次播放'),
                            'url': item['link'],
                        },
                    ))
                    time.sleep(0.5)  # 防止封IP
        return movie_list

    @staticmethod
    def batch(page: int, pagesize: int) -> [MovieEntity]:
        params: dict[str, any] = {
            'area': -1,
            'style_id': -1,
            'release_date': -1,
            'season_status': -1,
            'order': 2,
            'st': 2,
            'season_type': 2,
            'sort': 0,
            'page': page,
            'pagesize': pagesize,
            'type': 1,
        }
        response: Response = requests.get(url='https://api.bilibili.com/pgc/season/index/result',
                                          params=params,
                                          headers=headers)
        if response.status_code == 200:
            json_object: any = response.json()  # 解析数据
            write_in('bilibili', page, json_object)  # 缓存到json
            return Bilibili.handle_data(json_object)
        return []


class Tencent:
    base_url: str = "https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getPage?video_appid=3000010"
    cover_url: str = "https://v.qq.com/x/cover/{cid}.html"
    payload: {str: any} = {
        "page_context": {
            "page_index": "1"
        },
        "page_params": {
            "page_id": "channel_list_second_page",
            "page_type": "operation",
            "channel_id": "100173",
            "filter_params": "",
            "page": "1",
            "new_mark_label_enabled": "1",
        },
        "page_bypass_params": {
            "params": {
                "page_id": "channel_list_second_page",
                "page_type": "operation",
                "channel_id": "100173",
                "filter_params": "",
                "page": "1",
                "caller_id": "3000010",
                "platform_id": "2",
                "data_mode": "default",
                "user_mode": "default",
            },
            "scene": "operation",
            "abtest_bypass_id": "747ad9f34a4c8887",
        },
    }

    @staticmethod
    def get_movies(page: int = 0) -> [MovieEntity]:
        result: [MovieEntity] = []
        Tencent.payload["page_context"]["page_index"] = str(page)
        Tencent.payload["page_params"]["page"] = str(page)
        Tencent.payload["page_bypass_params"]["params"]["page"] = str(page)
        response = requests.post(Tencent.base_url, json=Tencent.payload)
        response.encoding = response.apparent_encoding
        json_object = json.loads(response.text)
        write_in('tencent', page, json_object)  # 缓存到json

        if json_object["ret"] != 0:
            err_msg: str = f"Error occurred when fetching movie data: {json_object['msg']}"
            logger.error(err_msg)
            raise err_msg

        for item in json_object["data"]["CardList"][1]["children_list"]["list"]["cards"]:
            cid: str = item["params"]["cid"]
            detail: {str: any} = Tencent.get_movie_detail(cid)
            result.append(
                MovieEntity(
                    title=item["params"]["title"],
                    cover_url=detail["cover_url"],
                    intro=detail["intro"],
                    score=detail['score'],
                    movie_type=[item["params"]["main_genre"]],
                    release_date=item['params']['publish_date'],
                    actors=detail['actors'],
                    metadata={
                        'source': 'Tencent',
                        'cid': cid,
                        'vid': detail['metadata']['vid'],
                        'area': item["params"]["area_name"],
                    },
                )
            )
            time.sleep(3)
        return result

    @staticmethod
    def get_movie_detail(cid: str) -> {str: any}:
        response = requests.get(Tencent.cover_url.format(cid=cid))
        response.encoding = response.apparent_encoding
        matches = re.findall(r"window\.__PINIA__=(.+)?</script>", response.text)[0]
        matches = (
            matches.replace("undefined", "null")
            .replace("Array.prototype.slice.call(", "")
            .replace("})", "}")
        )
        json_object: any = json.loads(matches)
        actor_list: [any] = json_object["introduction"]["starData"]["list"]
        score: float = safe_float_conversion(str(
            json.loads(json_object['introduction']
                       ['introData']
                       ['list'][0]
                       ['item_params']
                       ['imgtag_ver'])
            ['tag_4']['text']).removesuffix('分'))
        global_data = json_object["global"]
        cover_info = global_data["coverInfo"]
        return {
            'cover_url': cover_info["new_pic_vt"],
            'intro': cover_info["description"],
            'score': score,
            'release_date': cover_info["publish_date"],
            'actors': [star["star_name"] for star in actor_list],
            'metadata': {
                'vid': cover_info["video_ids"][0],
            },
        }

    @staticmethod
    def batch(page: int) -> [MovieEntity]:
        return Tencent.get_movies(page)


# class IQiYi:



# class DouBan:



def safe_float_conversion(value, default=-1.0):
    """尝试将给定的值转换为浮点数，如果失败则返回默认值"""
    try:
        return float(value)
    except ValueError:
        return default
