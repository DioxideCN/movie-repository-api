import uuid
from datetime import datetime

import requests
from lxml import etree

unique_id = str(uuid.uuid4())
timestamp = int(datetime.now().timestamp())

print(unique_id)
print(timestamp)
