from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any


class ResponseEnum(Enum):
    # 200
    RP_2000001 = (2000001, "查询成功")
    RP_2000002 = (2000002, "操作成功")  # 非直接修改resource操作
    RP_2000003 = (2000003, "修改成功")  # put/patch修改成僧

    # 201
    RP_2010001 = (2010001, "新增成功")  # 新增resource成功

    # 204
    RP_2040001 = (2040001, "操作成功")
    RP_2040002 = (2040002, "操作成功")

    # 209
    RP_2090001 = (2090001, "删除成功")  # 删除resource成功

    # 400
    RP_4000001 = (4000001, "缺少参数")  # 缺少必填参数
    RP_4000002 = (4000002, "参数非法")  # 参数值/类型不符合期望

    # 401
    RP_4010001 = (4010001, "鉴权失败")

    # 403
    RP_4030001 = (4030001, "权限不足")  # 没有call这个api的权限

    # 404
    RP_4040001 = (4040001, "资源不存在")

    # 409
    RP_4090001 = (4090001, "查询失败")
    RP_4090002 = (4090002, "新增失败")
    RP_4090003 = (4090003, "更新失败")
    RP_4090004 = (4090004, "删除失败")
    RP_4090005 = (4090005, "资源冲突")

    # 500
    RP_5000001 = (5000001, "服务器错误")  # 代码报错，根据具体场景决定是否返回报错信息
    RP_5000002 = (5000002, "其他错误")  # 并发量过高等

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.code}: {self.message}"


@dataclass
class ResultModel:
    code: int
    msg: str
    detail: Optional[str] = None
    data: Optional[Any] = None


class Response:
    @staticmethod
    def result(response: ResponseEnum, detail: Optional[str] = None, data: Optional[Any] = None) -> ResultModel:
        return ResultModel(
            code=response.code,
            msg=response.message,
            detail=detail,
            data=data
        )
