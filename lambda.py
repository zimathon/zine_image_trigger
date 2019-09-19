import os
import redis
import boto3
import json
import urllib.parse
import logging
import datetime
import secrets
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    # e.g. filename = "2_4_s3://aaaaabbbbbbaaaaaaaaaaaaaaaa"
    filename = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    timestamp = datetime.datetime.now().timestamp()

    dic = {}
    dic["class"] = "ZineImageWorker" 
    dic["args"] = str(filename)
    dic["retry"] = False
    dic["queue"] = "default"
    dic["jid"] = secrets.token_hex(12)
    dic["created_at"] = timestamp
    dic["enqueud_at"] = timestamp

    host = os.environ.get('HOSTNAME', 'localhost')
    r = redis.Redis(host=host, port=6379, db=0)
    r.lpush("sidekiq:queue:default", json.dumps(dic))

    return


if __name__ == "__main__":
    handler('','')