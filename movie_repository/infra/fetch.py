import asyncio
import json
import math
import re
from typing import Callable, Awaitable

import aiohttp
from lxml import etree

from movie_repository.util.logger import logger
from movie_repository.entity.entity_movie import MovieEntityV2, PlatformDetail
from .init_config import push_file_dump_msg, json_file, push_db_insert_msg
from .warmup import WarmupHandler, Status, generate_key
from movie_repository.util.default_util import StringUtil, TimeUtil

# 全局注册器
_task_run_registry = []
_rollback_run_registry = []
_rick_control_timeout = 8


class Platform:
    def __init__(self, name):
        self.name = name
        self.tasks = []

    async def run_tasks(self):
        for task in self.tasks:
            await task
            await asyncio.sleep(3)  # 每个任务延迟3秒后进入下一个任务


def inject(cls):
    _task_run_registry.append(cls)
    return cls


def checkpoint_rollback(cls):
    _rollback_run_registry.append(cls)
    return cls


async def execute_tasks(platform_tasks, log_message):
    logger.info(log_message)
    tasks = [platform.run_tasks() for platform in platform_tasks.values()]
    for platform_name, platform in platform_tasks.items():
        logger.info(f' -- {platform_name} of {len(platform.tasks)} task(s)')
    await asyncio.gather(*tasks)


async def run_all(warmup: WarmupHandler,
                  total: int):
    platforms: dict[str, Platform] = {name: Platform(name) for name in json_file}
    for cls in _task_run_registry:
        batches = math.ceil(total / cls.pagesize)
        if hasattr(cls, 'max_tasks') and cls.max_tasks < batches:
            batches = cls.max_tasks
        for page in range(1, batches + 1):  # scope [1, batches + 1)
            task = cls.run_async(warmup, page)
            platforms[cls.__name__.lower()].tasks.append(task)
    await execute_tasks(platforms,
                        f'Gathering {len(_task_run_registry)} running-type task(s): {_task_run_registry}')


async def rollback_all(warmup: WarmupHandler,
                       rollback_trace: dict[str, dict[str, int]]):
    platforms: dict[str, Platform] = {name: Platform(name) for name in json_file}
    for cls in _rollback_run_registry:
        key: str = cls.__name__.lower()
        if rollback_trace[key]:
            for task_id in rollback_trace[key]:
                # 从检点开始重试
                batch: int = rollback_trace[key][task_id]
                task = cls.run_async(warmup, batch, task_id)
                platforms[cls.__name__.lower()].tasks.append(task)
    await execute_tasks(platforms,
                        f'Gathering {len(_rollback_run_registry)} rollback-type task(s): {_rollback_run_registry}')


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
                                       page: int,
                                       pagesize: int,
                                       warmup: WarmupHandler,
                                       async_func: Callable[[], Awaitable[list[MovieEntityV2]]],
                                       retry_on_task_id: str = ''):
    logger.info(f"(Batch {page}*{pagesize}) Staring fetching response from platform '{platform}'.")
    # 生成 Batch Task ID
    unique_batch_id: str = retry_on_task_id if retry_on_task_id.startswith('BATCH.TASK') else generate_key('BATCH.TASK')
    batch_begin_time: str = TimeUtil.now()
    warmup.put_batch_trace(task_id=unique_batch_id, source=platform,
                           batch=page, pagesize=pagesize, status=Status.PENDING,
                           begin=batch_begin_time)
    batch_results: list[MovieEntityV2] = await async_func()  # 执行lambda
    push_db_insert_msg(batch_results)
    warmup.put_batch_trace(task_id=unique_batch_id, source=platform,
                           batch=page, pagesize=pagesize, status=Status.FINISHED,
                           begin=batch_begin_time, end=TimeUtil.now())


# B站数据源 ~ 通过控制反转注入到任务容器 min_page 0 total 5520
@inject
class Bilibili:
    max_tasks: int = 92
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
        'pagesize': pagesize,
        'type': 1,
    }

    @staticmethod
    async def fetch_actors(ep_id: str) -> list[str]:
        retries = 3  # 设置重试次数
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f'{Bilibili.actors_url}{ep_id}', headers=headers) as response:
                        if response.status == 200:
                            response_text = await response.text()
                            html = etree.HTML(response_text)
                            div_texts = html.xpath("//div[contains(text(), '出演演员')]//text()")
                            return filter_strings(''.join(t.strip() for t in div_texts)
                                                  .removeprefix('出演演员：').split('\n'))
                        else:
                            err = f"Failed to fetch data at platform {Bilibili.platform} ep_id {ep_id}."
                            logger.error(err)
                            raise Exception(f"{err} status code: ", response.status)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < retries - 1:  # 如果不是最后一次尝试，则等待后重试
                    await asyncio.sleep(2**attempt)  # 指数退避策略
                else:  # 最后一次尝试仍然失败，可以选择抛出异常或者其他错误处理
                    err = f"Failed to fetch actors after several retries at platform {Bilibili.platform} ep_id {ep_id}."
                    logger.error(err)
                    raise Exception(err) from e

    @staticmethod
    async def handle_data(raw_film_data: any) -> list[MovieEntityV2]:
        movie_list = []
        if 'data' in raw_film_data:
            data_film_data = raw_film_data['data']
            if 'list' in data_film_data:
                film_data = data_film_data['list']
                for item in film_data:  # 顺序处理每个项目并添加延迟
                    eq_id = str(item['first_ep']['ep_id'])
                    movie = await Bilibili.create_movie_entity(item, eq_id)
                    movie_list.append(movie)
                    await asyncio.sleep(_rick_control_timeout)  # 延迟阻塞退避风控
        return movie_list

    @staticmethod
    async def create_movie_entity(item: any, eq_id: str) -> MovieEntityV2:
        now_time: str = TimeUtil.now()
        actors = await Bilibili.fetch_actors(eq_id)
        return MovieEntityV2(  # 构造MovieEntityV2数据
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
    async def raw_fetch_async(page: int,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        Bilibili.params['page'] = page - 1
        async with aiohttp.ClientSession() as session:
            async with session.get(Bilibili.base_url,
                                   params=Bilibili.params,
                                   headers=headers) as response:
                if response.status == 200:
                    json_object = await response.json()
                    # 写入到json
                    push_file_dump_msg(Bilibili.platform, page, Bilibili.pagesize, json_object, retry_on_task_id)
                    return await Bilibili.handle_data(json_object)
        return []

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(Bilibili.platform,
                                           page,
                                           Bilibili.pagesize,
                                           warmup,
                                           lambda: Bilibili.raw_fetch_async(page),
                                           retry_on_task_id)


# 腾讯数据源 ~ 通过控制反转注入到任务容器 min_page 1 total 4980
@inject
class Tencent:
    max_tasks: int = 166
    pagesize: int = 30
    platform: str = 'tencent'
    base_url: str = 'https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getPage?video_appid=3000010'
    cover_url: str = 'https://v.qq.com/x/cover/{cid}.html'
    payload: dict[str: any] = {
        "page_context": {
            "page_index": "1"  # page默认从1开始
        },
        "page_params": {
            "page_id": "channel_list_second_page",
            "page_type": "operation",
            "channel_id": "100173",
            "filter_params": "",
            "page": "1",  # page默认从1开始
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
    async def get_movies(session: aiohttp.ClientSession,
                         page: int = 1,
                         retry_on_task_id: str = '') -> list[MovieEntityV2]:
        result: list[MovieEntityV2] = []
        Tencent.payload["page_context"]["page_index"] = str(page)
        Tencent.payload["page_params"]["page"] = str(page)
        Tencent.payload["page_bypass_params"]["params"]["page"] = str(page)
        async with session.post(Tencent.base_url, json=Tencent.payload) as response:
            response_text = await response.text()
            json_object = json.loads(response_text)
            push_file_dump_msg(Tencent.platform, page, 30, json_object, retry_on_task_id)  # 缓存到json
            if json_object["ret"] != 0:
                err_msg: str = f"Error occurred when fetching movie data: {json_object['msg']}"
                logger.error(err_msg)
                raise ValueError(err_msg)
            try:
                cards = json_object["data"]["CardList"][1]["children_list"]["list"]["cards"]
            except IndexError:
                cards = json_object["data"]["CardList"][0]["children_list"]["list"]["cards"]
            for item in cards:
                movie_detail = await Tencent.get_movie_detail(session, item["params"]["cid"], item)
                result.append(movie_detail)
                await asyncio.sleep(_rick_control_timeout)  # 延迟阻塞退避风控
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
            try:
                score = safe_float_conversion(json.loads(
                    json_object['introduction']['introData']['list'][0]['item_params']['imgtag_ver']
                )['tag_4']['text'].removesuffix('分'))
            except KeyError:
                score = -1.0
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
                    release_date=item['params'].get('epsode_pubtime', ''),
                    metadata={
                        'cid': cid,
                        'vid': cover_info["video_ids"][0],
                        'area': item["params"].get('area_name', '未知'),
                    }
                )],
            )

    @staticmethod
    async def raw_fetch_async(page: int,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        async with aiohttp.ClientSession() as session:
            movies = await Tencent.get_movies(session, page, retry_on_task_id)
            return movies

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(Tencent.platform,
                                           page,
                                           Tencent.pagesize,
                                           warmup,
                                           lambda: Tencent.raw_fetch_async(page, retry_on_task_id),
                                           retry_on_task_id)


# 爱奇艺数据源 ~ 通过控制反转注入到任务容器 min_page 1 total 780
@inject
class IQiYi:
    max_tasks: int = 13
    pagesize: int = 60
    platform: str = 'iqiyi'
    base_url = 'https://mesh.if.iqiyi.com/portal/lw/videolib/data'
    params: dict[str, str] = {
        "ret_num": "60",
        "channel_id": "1",
        "page_id": "1"  # 默认从1开始
    }

    @staticmethod
    async def raw_fetch_async(page: int = 1,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        async with aiohttp.ClientSession() as session:
            IQiYi.params['page_id'] = str(page)
            async with session.get(IQiYi.base_url, params=IQiYi.params, headers=headers) as response:
                json_object = await response.json()  # 直接获取JSON
                # 缓存到json
                push_file_dump_msg(IQiYi.platform, page, IQiYi.pagesize, json_object, retry_on_task_id)
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
                                cover_url=data['image_cover'],
                                create_time=now_time,
                                description=data['description'],
                                score=safe_float_conversion(data.get('sns_score', '')),
                                directors=[x['name'] for x in data['creator']],
                                actors=[x['name'] for x in data['contributor']],
                                movie_type=str(data.get('tag', '')).split(';'),
                                release_date=f'{data["date"]["year"]}-{data["date"]["month"]}-{data["date"]["day"]}',
                                metadata={
                                    'batch': page,
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
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(IQiYi.platform,
                                           page,
                                           IQiYi.pagesize,
                                           warmup,
                                           lambda: IQiYi.raw_fetch_async(page, retry_on_task_id),
                                           retry_on_task_id)


# 优酷数据源 ~ 注入到任务容器 min_page 1 total 4920
@inject
class YouKu:
    max_tasks: int = 82
    pagesize: int = 60
    platform: str = 'youku'
    base_url: str = ('https://www.youku.com/category/data?session=%7B%22subIndex%22%3A24%2C%22trackInfo%22%3A%7B%22'
                     'parentdrawerid%22%3A%2234441%22%7D%2C%22spmA%22%3A%22a2h05%22%2C%22level%22%3A2%2C%22spmC%22%'
                     '3A%22drawer3%22%2C%22spmB%22%3A%228165803_SHAIXUAN_ALL%22%2C%22index%22%3A1%2C%22pageName%22%'
                     '3A%22page_channelmain_SHAIXUAN_ALL%22%2C%22scene%22%3A%22search_component_paging%22%2C%22scmB'
                     '%22%3A%22manual%22%2C%22path%22%3A%22EP991816%2CEP991779%2CEP991776%2CEP991775%2CEP991774%22%'
                     '2C%22scmA%22%3A%2220140719%22%2C%22scmC%22%3A%2234441%22%2C%22from%22%3A%22SHAIXUAN%22%2C%22i'
                     'd%22%3A227939%2C%22category%22%3A%22%E7%94%B5%E5%BD%B1%22%7D&params=%7B%22type%22%3A%22%E7%94'
                     '%B5%E5%BD%B1%22%7D&pageNo=')
    youku_headers = {
        "Referer": "https://www.youku.com/category/show/type_%E7%94%B5%E5%BD%B1.html",
    }

    @staticmethod
    async def get_movie_info(obj_data: any):
        async with aiohttp.ClientSession() as session:
            async with session.get('https:' + obj_data["videoLink"]) as response:
                html = await response.text()
                j = re.findall(r"__INITIAL_DATA__ =(.+)?;<", html)[0]
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
                now_time: str = TimeUtil.now()
                return MovieEntityV2(  # 添加数据到结果集
                    _id=StringUtil.hash(obj_data["title"]),
                    fixed_title=obj_data["title"],
                    create_time=now_time,
                    update_time=now_time,
                    platform_detail=[PlatformDetail(
                        source=YouKu.platform,
                        title=obj_data["title"],
                        cover_url=item['cover_url'],
                        create_time=now_time,
                        description=item['intro'],
                        score=item['score'],
                        directors=item['directors'],
                        actors=item['actors'],
                        movie_type=item['movie_type'],
                        release_date=item['release_date'],
                        metadata={
                            'summary': obj_data
                        }
                    )],
                )

    @staticmethod
    async def raw_fetch_async(page: int = 1,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        async with aiohttp.ClientSession(headers=YouKu.youku_headers) as session:
            async with session.get(f'{YouKu.base_url}{page}') as response:
                json_object = await response.json()
                # 缓存到json
                push_file_dump_msg(YouKu.platform, page, YouKu.pagesize, json_object, retry_on_task_id)
                result: list[MovieEntityV2] = []
                results = json_object["data"]["filterData"]["listData"]
                for data in results:
                    fetch_res: MovieEntityV2 = await YouKu.get_movie_info(data)
                    result.append(fetch_res)
                    await asyncio.sleep(_rick_control_timeout)
                return result

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(YouKu.platform,
                                           page,
                                           YouKu.pagesize,
                                           warmup,
                                           lambda: YouKu.raw_fetch_async(page, retry_on_task_id),
                                           retry_on_task_id)


# 芒果数据源 ~ 注入到任务容器 min_page 1 total 2400
@inject
class MgTV:
    max_tasks: int = 40
    pagesize: int = 60
    platform: str = 'mgtv'
    base_url: str = 'https://pianku.api.mgtv.com/rider/list/pcweb/v3'
    params: dict[str, str | int] = {
        'allowedRC': 1,
        'platform': 'pcweb',
        'channelId': 3,
        'pn': 1,  # page 页号 默认从1开始
        'pc': pagesize,  # pagesize 页大小 默认80
        'hudong': 1,
        '_support': 10000000,
        'kind': 'a1',
        'edition': 'a1',
        'area': 'a1',
        'year': 'all',
        'chargeInfo': 'a1',
        'sort': 'c2'
    }

    @staticmethod
    async def raw_fetch_async(page: int = 1,
                              retry_on_task_id: str = '') -> list[MovieEntityV2]:
        MgTV.params['pn'] = page
        async with aiohttp.ClientSession() as session:
            async with session.get(MgTV.base_url, params=MgTV.params) as response:
                json_object = await response.json()  # 直接获取JSON
                # 缓存到json
                push_file_dump_msg(MgTV.platform, page, MgTV.pagesize, json_object, retry_on_task_id)
                result: list[MovieEntityV2] = []
                results = json_object['data']['hitDocs']
                now_time: str = TimeUtil.now()
                for data in results:
                    result.append(
                        MovieEntityV2(  # 添加数据到结果集
                            _id=StringUtil.hash(data['title']),
                            fixed_title=data['title'],
                            create_time=now_time,
                            update_time=now_time,
                            platform_detail=[PlatformDetail(
                                source=MgTV.platform,
                                title=data['title'],
                                cover_url=data['img'],
                                create_time=now_time,
                                description=data['story'],
                                score=safe_float_conversion(data.get('zhihuScore', '')),
                                actors=str(data['subtitle']).split(','),
                                movie_type=data['kind'],
                                release_date=data['year'],
                                metadata={
                                    'clip_id': data['clipId'],
                                    'views': data['views'],
                                    'update_time': data['se_updateTime'],
                                }
                            )],
                        )
                    )
                return result

    @staticmethod
    async def run_async(warmup: WarmupHandler,
                        page: int,
                        retry_on_task_id: str = ''):
        await abstract_run_with_checkpoint(MgTV.platform,
                                           page,
                                           MgTV.pagesize,
                                           warmup,
                                           lambda: MgTV.raw_fetch_async(page, retry_on_task_id),
                                           retry_on_task_id)
