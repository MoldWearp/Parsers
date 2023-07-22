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
    name = "GroupsByUser"

    def __init__(self, *args, **kwargs):
        self.api_url = "https://api.vk.com"
        self.api_version = 5.131
        self.access_token = os.getenv("TOKEN")

        self.group_fields = kwargs.get("group_fields") or ""
        self.group_extended = kwargs.get("group_extended") or 0

        self.id = kwargs.get("id")
        self.metadata = json.loads(kwargs["metadata"])

        logger.info(f"starting the spider with id {self.id} and spider name {self.name}")

        self.data = db.take_data("storage", self.id)
        self.users_id = self.data["UsersByGroup"]

        # self.url = f"{self.api_url}/method/groups.get/?user_id={self.user_id}&v={self.api_version}&extended={self.group_extended}&access_token={self.access_token}"

        super(UserSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        for user_id in self.users_id:
            url = f"{self.api_url}/method/groups.get/?user_id={user_id}&v={self.api_version}&extended={self.group_extended}&access_token={self.access_token}"
            yield scrapy.Request(url, self.parse, cb_kwargs={"user_id": user_id})

    def parse(self, response, **kwargs):
        user_id = kwargs.get("user_id")

        self.log(f"parse user's groups of user {user_id}")

        item = {}

        response = json.loads(response.text)

        if "response" in response:
            item["groupsByUser"] = response['response']
            item["user_id"] = user_id
        else:
            logger.debug(f"There is an error in response {response}")
            item["groupsByUser"] = {}
            item["user_id"] = user_id

        db.push_data("storage", item, self.id)
        item["metadata"] = json.loads(self.metadata)
        item["id"] = self.id
        yield item
