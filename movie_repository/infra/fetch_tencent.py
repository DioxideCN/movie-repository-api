import requests
import json
import re
import time

from movie_repository.entity import MovieEntity

base_url = "https://pbaccess.video.qq.com/trpc.vector_layout.page_view.PageService/getPage?video_appid=3000010"
params: {str: any} = {
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

cover_url = "https://v.qq.com/x/cover/{cid}.html"

"""
这里可选,使用常量来替换下面dataclass的movie_type的str类型
from enum import Enum
class MovieTypeEnum(Enum):
    动作 = 0
"""


def get_movies(page: int = 0) -> list[MovieEntity]:
    result = []
    params["page_context"]["page_index"] = str(page)
    params["page_params"]["page"] = str(page)
    params["page_bypass_params"]["params"]["page"] = str(page)
    response = requests.post(base_url, json=params)
    response.encoding = response.apparent_encoding
    json_object = json.loads(response.text)

    if json_object["ret"] != 0:
        print(json_object["msg"])
        raise "Error when fetching movie data"

    for item in json_object["data"]["CardList"][1]["children_list"]["list"]["cards"]:
        result.append(
            MovieEntity(
                title=item["params"]["title"],
                movie_type=item["params"]["main_genre"],
                cid=item["params"]["cid"],
                area=item["params"]["area_name"],
            )
        )
    return result


def get_movie_detail(movie_info: MovieInfo) -> None:
    response = requests.get(cover_url.format(cid=movie_info.cid))
    response.encoding = response.apparent_encoding
    j = re.findall(r"window\.__PINIA__=(.+)?</script>", response.text)[0]
    j = (
        j.replace("undefined", "null")
        .replace("Array.prototype.slice.call(", "")
        .replace("})", "}")
    )
    json_object = json.loads(j)
    star_data = json_object["introduction"]["starData"]["list"]
    global_data = json_object["global"]
    cover_info = global_data["coverInfo"]
    movie_info.cover = cover_info["new_pic_vt"]
    movie_info.vid = cover_info["video_ids"][0]
    movie_info.description = cover_info["description"]
    movie_info.publish_date = cover_info["publish_date"]

    stars: list[MovieStar] = []
    for star in star_data:
        stars.append(
            MovieStar(
                name=star["star_name"],
                pic=star["star_pic"],
                role=star["star_role_label"],
            )
        )
        movie_info.actors.append(star["star_name"])
    movie_info.stars = stars
    time.sleep(3)


if __name__ == "__main__":
    movies = get_movies()
    for movie in movies:
        get_movie_detail(movie)
    print(movies)
