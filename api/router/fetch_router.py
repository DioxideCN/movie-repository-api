from fastapi import APIRouter, Query

from api.entity.entity_response import Response, ResponseEnum, ResultModel
from api.infra.init_storage import collections
from api.util import logger

router = APIRouter()


@router.get("/movies", response_model=ResultModel)
async def read_movies(page: int = Query(1, gt=0),
                      pagesize: int = Query(20, gt=19)):
    return Response.result(ResponseEnum.RP_2000001, data=await get_paginated_movies(page, pagesize))


""" service functions below """


async def get_paginated_movies(page: int = 1, pagesize: int = 20):
    logger.info(f"'(/movies/) Getting {page} page of {pagesize} movie data(s).'")
    skip = (page - 1) * pagesize
    cursor = collections.find().skip(skip).limit(pagesize)
    movies = await cursor.to_list(length=pagesize)
    return movies
