import asyncio

# 全局注册器
class_registry = []


def task_scheduler(cls):
    class_registry.append(cls)
    return cls


async def run_all():
    tasks = []
    for cls in class_registry:
        total = 10
        iterations = total // cls.pagesize + 1
        for i in range(1, iterations + 1):
            tasks.append(cls.run_async(i))
    await asyncio.gather(*tasks)


@task_scheduler
class Bilibili:
    platform = 'Bilibili'
    pagesize = 5

    @staticmethod
    async def run_async(page):
        print(f'Fetching page {page} from {Bilibili.platform}')
        await asyncio.sleep(1)
        return f'Page {page} data'
