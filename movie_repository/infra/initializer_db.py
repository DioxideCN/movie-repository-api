import asyncio

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from . import fetcher

CLIENT = AsyncIOMotorClient("mongodb://localhost:27017/")  # 端
DB = CLIENT["movie_repository"]  # 库
COLLECTIONS: AsyncIOMotorCollection = DB["movie_collection"]  # 表


async def init_database(total: int, page_size: int):
    """
    从bilibili, tencent, douban平台获取电影信息
    并存入mongodb数据库的movie_collection数据表中
    """
    batch = total // page_size

    async def run_tasks():
        """
        从B站、豆瓣、腾讯分别爬取1000条数据，该过程理应通过异步来执行
        数据量较大，在程序中通过TOTAL和PAGE_SIZE来切分成不同的子任务
        在这里将设计为协程函数调用的形式来防止阻塞产生的严重耗时，参考：
        1. TypeScript await/async 异步流
        2. Java VirtualThread 虚拟线程池
        3. Java ForkJoinPool 工作窃取线程池(基于平台线程)
        """
        tasks = []
        for page in range(1, batch + 1):
            tasks.append(fetcher.Bilibili.run_async(COLLECTIONS, page, page_size))
            # tasks.append(fetcher.Tencent.run_async(COLLECTIONS, page))
        await asyncio.gather(*tasks)

    await run_tasks()
