import os
import json
import logging
import requests
import gpt4

class ScoutBot(object):
    def __init__(self):
        pass

    def process_message(self, content):
        content = json.loads(content)
        # content["text"] = "This is Scout. " + content['text']
        reply = gpt4.entrance(content["text"])
        content["text"] = reply
        content = json.dumps(content)
        return content