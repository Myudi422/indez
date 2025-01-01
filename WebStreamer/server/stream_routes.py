# Taken from megadlbot_oss <https://github.com/eyaadh/megadlbot_oss/blob/master/mega/webserver/routes.py>
# Thanks to Eyaadh <https://github.com/eyaadh>

import re
import time
import math
import logging
import secrets
import mimetypes
from collections import OrderedDict
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from WebStreamer.bot import multi_clients, work_loads
from WebStreamer.server.exceptions import FIleNotFound, InvalidHash
from WebStreamer import Var, utils, StartTime, __version__, StreamBot

routes = web.RouteTableDef()

# Caching setup
class_cache = OrderedDict()
CACHE_LIMIT = 100  # Cache limit for ByteStreamer objects

def add_to_cache(client, streamer):
    if len(class_cache) >= CACHE_LIMIT:
        class_cache.popitem(last=False)  # Remove oldest cached item
    class_cache[client] = streamer

@routes.get("/", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": utils.get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + StreamBot.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": __version__,
        }
    )

@routes.get(r"/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            message_id = int(match.group(2))
        else:
            message_id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
        return await media_streamer(request, message_id)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))

async def select_client():
    client_latencies = await asyncio.gather(
        *(utils.ping_client(client) for client in multi_clients)
    )
    return multi_clients[client_latencies.index(min(client_latencies))]

def parse_range_header(range_header, file_size):
    try:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
        if from_bytes < 0 or until_bytes >= file_size or from_bytes > until_bytes:
            raise ValueError
        return from_bytes, until_bytes
    except Exception:
        raise web.HTTPBadRequest(text="Invalid Range Header")

async def media_streamer(request: web.Request, message_id: int):
    range_header = request.headers.get("Range", 0)
    faster_client = await select_client()

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logging.debug(f"Using cached ByteStreamer object for client {faster_client}")
    else:
        logging.debug(f"Creating new ByteStreamer object for client {faster_client}")
        tg_connect = utils.ByteStreamer(faster_client)
        add_to_cache(faster_client, tg_connect)

    file_id = await tg_connect.get_file_properties(message_id)
    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = parse_range_header(range_header, file_size)
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    req_length = until_bytes - from_bytes + 1
    body = tg_connect.yield_file(
        file_id, from_bytes, until_bytes, chunk_size
    )
    mime_type = file_id.mime_type
    file_name = file_id.file_name or f"{secrets.token_hex(2)}.unknown"
    disposition = "inline" if "video/" in mime_type or "audio/" in mime_type or "/html" in mime_type else "attachment"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )
