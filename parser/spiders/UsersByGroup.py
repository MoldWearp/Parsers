import json
import os
import pprint
from parser.settings import db

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

        self.id = self.settings["unique_id"]

        self.data = db.take_data("storage", self.id)
        self.group_id = self.data["group"]["id"]
        self.metadata = kwargs["metadata"]

        self.url = f"{self.api_url}/method/groups.getMembers/?group_id={self.group_id}&offset={self.offset}&v={self.api_version}" \
                   f"&access_token={self.access_token}"

        super(UserSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        yield scrapy.Request(self.url, self.parse)

    def parse(self, response, **kwargs):
        self.log(f"parse users of {self.group_id} group")
        item = {"metadata": self.metadata}
        users = json.loads(response.text)['response']
        item["groupsByUser"] = users

        db.update(item)

        yield item

