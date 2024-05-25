import asyncio

import aiohttp

from movie_repository.entity import MovieEntity


def safe_float_conversion(value, default=-1.0):
    """尝试将给定的值转换为浮点数，如果失败则返回默认值"""
    try:
        return float(value)
    except ValueError:
        return default


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
    async def get_movies(page: int = 0) -> list[MovieEntity]:
        async with aiohttp.ClientSession() as session:
            IQiYi.params['page_id'] = str(page)
            async with session.get(IQiYi.base_url, params=IQiYi.params) as response:
                json_object = await response.json()  # 直接获取JSON
                result: list[MovieEntity] = []
                results = json_object["data"]
                for data in results:
                    result.append(
                        MovieEntity(
                            title=data["title"],
                            cover_url=data["album_image_url_hover"],
                            directors=[x["name"] for x in data["creator"]],
                            actors=[x["name"] for x in data["contributor"]],
                            release_date=f'{data["date"]["year"]}-{data["date"]["month"]}-{data["date"]["day"]}',
                            intro=data["description"],
                            score=safe_float_conversion(data.get("sns_score", 0.0)),
                            movie_type=data.get("tag", ""),
                            metadata=data,
                        )
                    )
                return result


# 假设 get_movies 已经定义并且准备好使用
async def main():
    movies = await IQiYi.get_movies()  # 调用异步函数并传递参数
    print(movies[0])  # 打印结果

# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
