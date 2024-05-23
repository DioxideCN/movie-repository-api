import requests
from requests import Response

from movie_repository.entity import MovieEntity

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}

movie_types: dict[str, str] = {
    '剧情': '11',
    '戏剧': '24',
    '动作': '5',
    '爱情': '13',
    '科技': '17'
}


def handle_data(raw_film_data: [any]) -> [MovieEntity]:
    movie_list: [MovieEntity] = []
    for item in raw_film_data:
        movie_list.append(MovieEntity(
            title=item['title'],
            cover_url=item['cover_url'],
            score=float(item.get('score', '-1')),
            movie_type=item['types'],
            actors=item['actors'],
            release_date=item['release_date'],
            metadata={
                'source': '豆瓣',
                'url': item['url'],
            },
        ))
    return movie_list


def fetch_douban(total: int) -> [str]:
    per_num: int = total // len(movie_types) +1
    params: dict[str, str | int] = {
        'type': '',  # 电影类型
        'interval_id': '100:90',
        'action': '',
        'start': '0',  # 起始电影iD
        'limit': per_num  # 数量
    }
    result_list: [MovieEntity] = []
    for genre, type_id in movie_types.items():
        params['type']: str = type_id
        response: Response = requests.get(url='https://movie.douban.com/j/chart/top_list',
                                          params=params,
                                          headers=headers)
        # 确保请求成功
        if response.status_code == 200:
            raw_film_data: [any] = response.json()  # 解析数据
            result_list.extend(film_data)  # 将当前类型的电影数据添加到列表中
    return result_list


print(fetch_douban(10))
