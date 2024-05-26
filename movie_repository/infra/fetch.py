import asyncio
import json
import re
from datetime import datetime
from typing import Callable, Awaitable

import aiohttp
from lxml import etree
from motor.motor_asyncio import AsyncIOMotorCollection

from movie_repository.util.logger import logger
from movie_repository.entity.entity_movie import MovieEntityV2, PlatformDetail
from .init_config import write_in
from .warmup import WarmupHandler, Status, generate_key
from movie_repository.util.default_util import StringUtil, TimeUtil, MongoUtil

# 全局注册器
_task_run_registry = []
_rollback_run_registry = []


def inject(cls):
    _task_run_registry.append(cls)
    return cls


def checkpoint_rollback(cls):
    _rollback_run_registry.append(cls)
    return cls


async def run_all(warmup: WarmupHandler,
                  collection: AsyncIOMotorCollection,
                  total: int):
    tasks = []
    for cls in _task_run_registry:
        batches = total // cls.pagesize
        for page in range(1, batches + 1):
            tasks.append(cls.run_async(warmup, collection, page))
    await asyncio.gather(*tasks)


async def rollback_all(warmup: WarmupHandler,
                       collection: AsyncIOMotorCollection,
                       rollback_trace: dict[str, dict[str, int]]):
    tasks = []
    for cls in _task_run_registry:
        key: str = cls.__name__.lower()
        if rollback_trace[key]:
            for task_id in rollback_trace[key]:
                # 从检点开始重试
                batch: int = rollback_trace[key][task_id]
                logger.info(f'Checkpoint Rollback -> found rollback checkpoint at {key} platform {batch} batch.')
                tasks.append(cls.run_async(warmup, collection, batch, task_id))
    await asyncio.gather(*tasks)


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


def filter_strings(strings: [str]) -> [str]:
    # 使用列表推导式过滤字符串
    filtered_strings = [s for s in strings if s and "《" not in s and "》" not in s]
    return filtered_strings


async def abstract_run_with_checkpoint(platform: str,
                                       collection: AsyncIOMotorCollection,
                                       page: int,
                                       pagesize: int,
                                       warmup: WarmupHandler,
                                       async_func: Callable[[], Awaitable[list[MovieEntityV2]]],
                                       retry_on_task_id: str = ''):
    logger.info(f'[Batch {page}] Fetching {platform} data...')
    # Batch Task Key
    batch_task_id: str = retry_on_task_id if retry_on_task_id.startswith('BATCH.TASK') else generate_key('BATCH.TASK')
    batch_begin_time: str = TimeUtil.now()
    # 缓存预热事件点
    warmup.put_batch_trace(task_id=batch_task_id, source=platform,
                           batch=page, pagesize=pagesize, status=Status.PENDING,
                           begin=batch_begin_time)
    batch_results = await async_func()
    warmup.put_batch_trace(task_id=batch_task_id, source=platform,
                           batch=page, pagesize=pagesize, status=Status.FINISHED,
                           begin=batch_begin_time, end=datetime.now().strftime(TimeUtil.now()))
    logger.info(f'[Batch {page}] Inserting {platform} data into mongodb')
    # DB Task Key
    db_task_id: str = retry_on_task_id if retry_on_task_id.startswith('DB.TASK') else generate_key('DB.TASK')
    db_begin_time: str = TimeUtil.now()
    warmup.put_db_trace(task_id=db_task_id, platform=platform,
                        batch=page, pagesize=pagesize, status=Status.PENDING,
                        begin=db_begin_time)
    await MongoUtil.batch_put(collection, batch_results)
    warmup.put_db_trace(task_id=db_task_id, platform=platform,
                        batch=page, pagesize=pagesize, status=Status.FINISHED,
                        begin=db_begin_time, end=datetime.now().strftime(TimeUtil.now()))
    await asyncio.sleep(1)  # QOS缓冲


# B站数据源 ~ 通过控制反转注入到任务容器
@inject
class Bilibili:
    pagesize: int = 60
    platform: str = 'bilibili'
    base_url: str = 'https://api.bilibili.com/pgc/season/index/result'
    actors_url: str = 'https://www.bilibili.com/bangumi/play/ep'
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
            async with session.get(f'{Bilibili.actors_url}{ep_id}', headers=headers) as response:
                response_text = await response.text()
                html = etree.HTML(response_text)
                div_texts = html.xpath("//div[contains(text(), '出演演员')]//text()")
                return filter_strings(''.join(t.strip() for t in div_texts).removeprefix('出演演员：').split('\n'))

    @staticmethod
    async def handle_data(raw_film_data: any) -> list[MovieEntityV2]:
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
    async def create_movie_entity(item: any, eq_id: str) -> MovieEntityV2:
        now_time: str = TimeUtil.now()
        actors = await Bilibili.fetch_actors(eq_id)
        return MovieEntityV2(  # 添加数据到结果集
            _id=StringUtil.hash(item['title']),
            fixed_title=item['title'],
            create_time=now_time,
            update_time=now_time,
            platform_detail=[PlatformDetail(
                source=Bilibili.platform,
                title=item['title'],
                cover_url=item['cover'],
                create_time=now_time,
                description=item.get('subTitle', ''),
                score=safe_float_conversion(item.get('score', ''), -1.0),
                actors=actors,
                release_date=str(item['index_show']).removesuffix('上映'),
                metadata={
                    'id': eq_id,
                    'order': str(item['order']).removesuffix('次播放'),
                    'url': item['link'],
                }
            )],
        )

    @staticmethod
    async def raw_fetch_async(warmup: WarmupHandler,
                              page: int,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        Bilibili.params['page'] = page
        Bilibili.params['pagesize'] = Bilibili.pagesize
        async with aiohttp.ClientSession() as session:
            async with session.get(Bilibili.base_url,
                                   params=Bilibili.params,
                                   headers=headers) as response:
                if response.status == 200:
                    json_object = await response.json()  # 解析数据
                    # 缓存到json
                    write_in(warmup, Bilibili.platform, page, Bilibili.pagesize, json_object, retry_on_task_id)
                    return await Bilibili.handle_data(json_object)
        return []

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        collection: AsyncIOMotorCollection,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(Bilibili.platform,
                                           collection,
                                           page,
                                           Bilibili.pagesize,
                                           warmup,
                                           lambda: Bilibili.raw_fetch_async(warmup, page),
                                           retry_on_task_id)


# 腾讯数据源 ~ 通过控制反转注入到任务容器
@inject
class Tencent:
    pagesize: int = 30
    platform: str = 'tencent'
    base_url: str = 'https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getPage?video_appid=3000010'
    cover_url: str = 'https://v.qq.com/x/cover/{cid}.html'
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
    async def get_movies(warmup: WarmupHandler,
                         session: aiohttp.ClientSession,
                         page: int = 0,
                         retry_on_task_id: str = '') -> list[MovieEntityV2]:
        result: list[MovieEntityV2] = []
        Tencent.payload["page_context"]["page_index"] = str(page)
        Tencent.payload["page_params"]["page"] = str(page)
        Tencent.payload["page_bypass_params"]["params"]["page"] = str(page)
        async with session.post(Tencent.base_url, json=Tencent.payload) as response:
            response_text = await response.text()
            json_object = json.loads(response_text)
            write_in(warmup, Tencent.platform, page, 30, json_object, retry_on_task_id)  # 缓存到json
            if json_object["ret"] != 0:
                err_msg: str = f"Error occurred when fetching movie data: {json_object['msg']}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            try:
                cards = json_object["data"]["CardList"][1]["children_list"]["list"]["cards"]
            except IndexError:
                cards = json_object["data"]["CardList"][0]["children_list"]["list"]["cards"]
            tasks = [
                Tencent.get_movie_detail(session, item["params"]["cid"], item)
                for item in cards
            ]
            movie_entities = await asyncio.gather(*tasks)
            result.extend(movie_entities)
        return result

    @staticmethod
    async def get_movie_detail(session: aiohttp.ClientSession, cid: str, item: dict[str, any]) -> MovieEntityV2:
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
            global_data: any = json_object["global"]
            cover_info: any = global_data["coverInfo"]
            now_time: str = TimeUtil.now()
            return MovieEntityV2(  # 添加数据到结果集
                _id=StringUtil.hash(item["params"]["title"]),
                fixed_title=item["params"]["title"],
                create_time=now_time,
                update_time=now_time,
                platform_detail=[PlatformDetail(
                    source=Tencent.platform,
                    title=item["params"]["title"],
                    cover_url=cover_info["new_pic_vt"],
                    create_time=now_time,
                    description=cover_info["description"],
                    score=score,
                    actors=[star["star_name"] for star in actor_list],
                    movie_type=[item["params"]["main_genre"]],
                    release_date=item['params']['epsode_pubtime'],
                    metadata={
                        'cid': cid,
                        'vid': cover_info["video_ids"][0],
                        'area': item["params"]["area_name"],
                    }
                )],
            )

    @staticmethod
    async def raw_fetch_async(warmup: WarmupHandler,
                              page: int,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        async with aiohttp.ClientSession() as session:
            return await Tencent.get_movies(warmup, session, page, retry_on_task_id)

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        collection: AsyncIOMotorCollection,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(Tencent.platform,
                                           collection,
                                           page,
                                           Tencent.pagesize,
                                           warmup,
                                           lambda: Tencent.raw_fetch_async(warmup, page, retry_on_task_id),
                                           retry_on_task_id)


# 爱奇艺数据源 ~ 通过控制反转注入到任务容器
@inject
class IQiYi:
    pagesize: int = 24
    platform: str = 'iqiyi'
    base_url = 'https://mesh.if.iqiyi.com/portal/lw/videolib/data'
    params: dict[str, str] = {
        "ret_num": "60",
        "channel_id": "1",
        "page_id": "0"  # 默认从0开始
    }

    @staticmethod
    async def raw_fetch_async(warmup: WarmupHandler,
                              page: int = 0,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        async with aiohttp.ClientSession() as session:
            IQiYi.params['page_id'] = str(page)
            async with session.get(IQiYi.base_url, params=IQiYi.params) as response:
                json_object = await response.json()  # 直接获取JSON
                # 缓存到json
                write_in(warmup, IQiYi.platform, page, 30, json_object, retry_on_task_id)
                result: list[MovieEntityV2] = []
                results = json_object["data"]
                now_time: str = TimeUtil.now()
                for data in results:
                    result.append(
                        MovieEntityV2(  # 添加数据到结果集
                            _id=StringUtil.hash(data['title']),
                            fixed_title=data['title'],
                            create_time=now_time,
                            update_time=now_time,
                            platform_detail=[PlatformDetail(
                                source=IQiYi.platform,
                                title=data['title'],
                                cover_url=data['album_image_url_hover'],
                                create_time=now_time,
                                description=data['description'],
                                score=safe_float_conversion(data.get('sns_score', '')),
                                directors=[x['name'] for x in data['creator']],
                                actors=[x['name'] for x in data['contributor']],
                                movie_type=str(data.get('tag', '')).split(';'),
                                release_date=f'{data["date"]["year"]}-{data["date"]["month"]}-{data["date"]["day"]}',
                                metadata={
                                    'batch_order': data['order'],
                                    'entity_id': data['entity_id'],
                                    'album_id': data['album_id'],
                                    'tv_id': data['tv_id'],
                                }
                            )],
                        )
                    )
                return result

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        collection: AsyncIOMotorCollection,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(IQiYi.platform,
                                           collection,
                                           page,
                                           IQiYi.pagesize,
                                           warmup,
                                           lambda: IQiYi.raw_fetch_async(warmup, page, retry_on_task_id),
                                           retry_on_task_id)


# 优酷数据源 ~ 注入到任务容器
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


# 豆瓣数据源 ~ 注入到任务容器
class DouBan:
    pagesize: int = 60
    platform: str = 'douban'
    base_url: str = "https://mesh.if.iqiyi.com/portal/lw/videolib/data?ret_num=60&channel_id=1&page_id={PAGE_ID}"
