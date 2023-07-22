import json
import os
import pprint
from parser.settings import db
from loguru import logger
import scrapy
from dotenv import load_dotenv

pp = pprint.PrettyPrinter()
load_dotenv()


class UserSpider(scrapy.Spider):
    name = "UsersByGroup"

    def __init__(self, *args, **kwargs):
        self.api_url = "https://api.vk.com"
        self.api_version = 5.131
        self.access_token = os.getenv("TOKEN")

        self.offset = kwargs.get('offset') or 0
        self.group_fields = kwargs.get("group_fields") or ""
        self.group_extended = kwargs.get("group_extended") or 0

        self.id = kwargs.get("id")
        self.metadata = json.loads(kwargs["metadata"])

        logger.info(f"starting the spider with id {self.id} and spider name {self.name}")

        self.data = db.take_data("storage", self.id)
        self.group_id = self.data["group"]["id"]

        self.url = f"{self.api_url}/method/groups.getMembers/?group_id={self.group_id}&offset={self.offset}&v={self.api_version}" \
                   f"&access_token={self.access_token}"

        super(UserSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        yield scrapy.Request(self.url, self.parse)

    def parse(self, response, **kwargs):
        self.log(f"parse users of {self.group_id} group")
        item = {}

        response = json.loads(response.text)
        if "response" in response:
            item["UsersByGroup"] = response['response']["items"]
        else:
            logger.debug(f"There is an error in response {response}")
            item["UsersByGroup"] = {}

        db.update("storage", item, self.id)
        item["metadata"] = json.loads(self.metadata)
        item["id"] = self.id
        yield item

