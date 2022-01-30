# coding=utf-8
import json
import uuid
import requests


class Aria2:
    def __init__(self, scheme, host, port, token):
        self.attr = []
        self.token = token
        self.rpc_id = str(uuid.uuid1()).split('-')[0]
        self.rpc_url = "{}://{}:{}/jsonrpc".format(scheme, host, port)

    def set_default_attr(self, *attr):
        self.attr = attr

    # Remove all finish task
    def purgeDownloadResult(self):
        json_data = self.rpc_json("purgeDownloadResult")
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)

    # Remove all finish task
    def removeDownloadResult(self, gid: str):
        json_data = self.rpc_json("removeDownloadResult", str(gid))
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)["result"]

    # Remove all finish task
    def addUri(self, urls: list):
        json_data = self.rpc_json("addUri", urls)
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)["result"]

    # Remove all finish task
    def tellActive(self, attr=None):
        if attr is None:
            attr = self.attr
        json_data = self.rpc_json("tellActive", attr)
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)["result"]

    # Remove all finish task
    def tellWaiting(self, attr=None):
        if attr is None:
            attr = self.attr
        json_data = self.rpc_json("tellWaiting", 0, 1000, attr)
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)["result"]

    # Remove all finish task
    def tellStopped(self, attr: list = None):
        if attr is None:
            attr = self.attr
        json_data = self.rpc_json("tellStopped", -1, 1000, attr)
        res = requests.post(self.rpc_url, json=json_data)
        return json.loads(res.text)["result"]

    def rpc_json(self, func: str, *params):
        return {
            "jsonrpc": "2.0",
            "method": "aria2.{}".format(func),
            "id": self.rpc_id,
            "params":
                [
                    "token:{}".format(self.token),
                    *params
                ]
        }
