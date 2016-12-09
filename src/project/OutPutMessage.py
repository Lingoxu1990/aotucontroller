
import struct
import json
from io import BytesIO


def getBytes(json_str):
    encodedjson = json.dumps(json_str)

    byteio = BytesIO()

    encodedjson.encode('utf-8')

    lenofjson = len(encodedjson.encode('utf-8'))

    byteio.write(struct.pack('i', 8 + lenofjson))
    byteio.write(struct.pack('i', 2))
    byteio.write(encodedjson.encode('utf-8'))
    return byteio.getvalue()




