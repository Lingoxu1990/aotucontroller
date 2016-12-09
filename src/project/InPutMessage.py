import struct
import json
from io import BytesIO


def getJson(param):

    package_len,package_type = struct.unpack("ii",buffer(param[0:8]))
    stringbufferbytes = param[8:package_len]
    recv_json_str = stringbufferbytes.decode('utf-8')
    recv_json= json.loads(recv_json_str)
    return recv_json


