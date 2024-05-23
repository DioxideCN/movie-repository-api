import time

import requests
from requests import Response
from lxml import etree

from movie_repository.entity import MovieEntity
from .initializer_config import write_in

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}


def resolve_ep_id(link: str) -> str:
    return link.split('/bangumi/play/')[-1].split('?')[0]


def fetch_actors(ep_id: str) -> [str]:
    response: Response = requests.get(f'https://www.bilibili.com/bangumi/play/ep{ep_id}', headers=headers)
    response.encoding = 'utf-8'
    html = etree.HTML(response.text)
    div_texts = html.xpath("//div[contains(text(), '出演演员')]//text()")
    return ''.join(t.strip() for t in div_texts).removeprefix('出演演员：').split('\n')


def handle_data(raw_film_data: any) -> [MovieEntity]:
    movie_list: [MovieEntity] = []
    if 'data' in raw_film_data:
        data_film_data: any = raw_film_data['data']
        if 'list' in data_film_data:
            film_data: [any] = data_film_data['list']
            for item in film_data:  # 遍历JSON结果转换为MovieEntity对象
                eq_id = item['first_ep']['ep_id']
                movie_list.append(MovieEntity(  # 添加数据到结果集
                    title=item['title'],
                    cover_url=item['cover'],
                    intro=item.get('subTitle', ''),
                    score=safe_float_conversion(item.get('score', ''), -1.0),
                    actors=fetch_actors(eq_id),
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


def batch_fetch(page: int, pagesize: int) -> [MovieEntity]:
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
        raw_film_data: any = response.json()  # 解析数据
        write_in('bilibili', page, raw_film_data)  # 缓存到json
        return handle_data(raw_film_data)
    return []


def safe_float_conversion(value, default=-1.0):
    """尝试将给定的值转换为浮点数，如果失败则返回默认值"""
    try:
        return float(value)
    except ValueError:
        return default

