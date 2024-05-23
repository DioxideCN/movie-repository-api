import asyncio
import json
import time
import re
from dataclasses import asdict

import aiohttp
import requests
from lxml import etree
from motor.motor_asyncio import AsyncIOMotorCollection

from movie_repository.util.logger import logger
from movie_repository.entity import MovieEntity
from .initializer_config import write_in

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}


def safe_float_conversion(value, default=-1.0):
    """尝试将给定的值转换为浮点数，如果失败则返回默认值"""
    try:
        return float(value)
    except ValueError:
        return default


class Bilibili:
    params = {
        'area': -1,
        'style_id': -1,
        'release_date': -1,
        'season_status': -1,
        'order': 2,
        'st': 2,
        'season_type': 2,
        'sort': 0,
        'page': 0,
        'pagesize': 0,
        'type': 1,
    }

    @staticmethod
    async def fetch_actors(ep_id: str) -> list[str]:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'https://www.bilibili.com/bangumi/play/ep{ep_id}', headers=headers) as response:
                response_text = await response.text()
                html = etree.HTML(response_text)
                div_texts = html.xpath("//div[contains(text(), '出演演员')]//text()")
                return ''.join(t.strip() for t in div_texts).removeprefix('出演演员：').split('\n')

    @staticmethod
    async def handle_data(raw_film_data: any) -> list[MovieEntity]:
        movie_list = []
        if 'data' in raw_film_data:
            data_film_data = raw_film_data['data']
            if 'list' in data_film_data:
                film_data = data_film_data['list']
                tasks = []
                for item in film_data:  # 遍历JSON结果转换为MovieEntity对象
                    eq_id = str(item['first_ep']['ep_id'])
                    tasks.append(Bilibili.create_movie_entity(item, eq_id))
                movie_list = await asyncio.gather(*tasks)
        return movie_list

    @staticmethod
    async def create_movie_entity(item: any, eq_id: str) -> MovieEntity:
        actors = await Bilibili.fetch_actors(eq_id)
        return MovieEntity(  # 添加数据到结果集
            title=item['title'],
            cover_url=item['cover'],
            intro=item.get('subTitle', ''),
            score=safe_float_conversion(item.get('score', ''), -1.0),
            actors=actors,
            release_date=str(item['index_show']).removesuffix('上映'),
            metadata={
                'source': 'Bilibili',
                'id': eq_id,
                'order': str(item['order']).removesuffix('次播放'),
                'url': item['link'],
            },
        )

    @staticmethod
    async def raw_fetch_async(page: int, pagesize: int) -> list[MovieEntity]:
        Bilibili.params['page'] = page
        Bilibili.params['pagesize'] = pagesize
        async with aiohttp.ClientSession() as session:
            async with session.get('https://api.bilibili.com/pgc/season/index/result',
                                   params=Bilibili.params,
                                   headers=headers) as response:
                if response.status == 200:
                    json_object = await response.json()  # 解析数据
                    write_in('bilibili', page, json_object)  # 缓存到json
                    return await Bilibili.handle_data(json_object)
        return []

    @staticmethod
    async def run_async(collection: AsyncIOMotorCollection,
                        page: int,
                        page_size: int) -> list[dict[str, any]]:
        logger.info(f'[Batch {page}] Fetching bilibili data...')
        batch_result_bilibili = await Bilibili.raw_fetch_async(page, page_size)
        documents = [asdict(movie) for movie in batch_result_bilibili]
        logger.info(f'[Batch {page}] Inserting bilibili data into mongodb')
        await collection.insert_many(documents)
        await asyncio.sleep(0.5)  # QOS缓冲
        return documents


class Tencent:
    base_url: str = "https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getPage?video_appid=3000010"
    cover_url: str = "https://v.qq.com/x/cover/{cid}.html"
    payload: dict[str: any] = {
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
    async def get_movies(session: aiohttp.ClientSession, page: int = 0) -> list[MovieEntity]:
        result: list[MovieEntity] = []
        Tencent.payload["page_context"]["page_index"] = str(page)
        Tencent.payload["page_params"]["page"] = str(page)
        Tencent.payload["page_bypass_params"]["params"]["page"] = str(page)
        async with session.post(Tencent.base_url, json=Tencent.payload) as response:
            response_text = await response.text()
            json_object = json.loads(response_text)
            write_in('tencent', page, json_object)  # 缓存到json
            if json_object["ret"] != 0:
                err_msg: str = f"Error occurred when fetching movie data: {json_object['msg']}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            tasks = [
                Tencent.get_movie_detail(session, item["params"]["cid"], item)
                for item in json_object["data"]["CardList"][1]["children_list"]["list"]["cards"]
            ]
            movie_entities = await asyncio.gather(*tasks)
            result.extend(movie_entities)
        return result

    @staticmethod
    async def get_movie_detail(session: aiohttp.ClientSession, cid: str, item: dict[str, any]) -> MovieEntity:
        async with session.get(Tencent.cover_url.format(cid=cid)) as response:
            response_text = await response.text()
            matches = re.findall(r"window\.__PINIA__=(.+)?</script>", response_text)[0]
            matches = (
                matches.replace("undefined", "null")
                .replace("Array.prototype.slice.call(", "")
                .replace("})", "}")
            )
            json_object: any = json.loads(matches)
            actor_list: list[any] = json_object["introduction"]["starData"]["list"]
            score: float = safe_float_conversion(str(
                json.loads(json_object['introduction']
                           ['introData']
                           ['list'][0]
                           ['item_params']
                           ['imgtag_ver'])
                ['tag_4']['text']).removesuffix('分'))
            global_data = json_object["global"]
            cover_info = global_data["coverInfo"]

            return MovieEntity(
                title=item["params"]["title"],
                cover_url=cover_info["new_pic_vt"],
                intro=cover_info["description"],
                score=score,
                movie_type=[item["params"]["main_genre"]],
                release_date=item['params']['publish_date'],
                actors=[star["star_name"] for star in actor_list],
                metadata={
                    'source': 'Tencent',
                    'cid': cid,
                    'vid': cover_info["video_ids"][0],
                    'area': item["params"]["area_name"],
                },
            )

    @staticmethod
    async def raw_fetch_async(page: int) -> list[MovieEntity]:
        async with aiohttp.ClientSession() as session:
            return await Tencent.get_movies(session, page)

    @staticmethod
    async def run_async(collection: AsyncIOMotorCollection,
                        page: int) -> list[dict[str, any]]:
        logger.info(f'[Batch {page}] Fetching tencent data...')
        batch_result_bilibili = await Tencent.raw_fetch_async(page)
        documents = [asdict(movie) for movie in batch_result_bilibili]
        logger.info(f'[Batch {page}] Inserting tencent data into mongodb')
        await collection.insert_many(documents)
        await asyncio.sleep(0.5)  # QOS缓冲
        return documents


# class IQiYi:



# class DouBan:


