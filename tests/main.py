from datetime import datetime
from enum import Enum


time_formatter: str = '%Y-%m-%d %H:%M:%S,%f'

print(datetime.now().strftime(time_formatter)[:-3])
