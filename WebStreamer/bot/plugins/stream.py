# Ini adalah bagian dari TG-FileStreamBot
# Kode: Jyothis Jayanth [@EverythingSuckz]

import logging
from pyrogram import filters
from WebStreamer.vars import Var
from urllib.parse import quote_plus
from WebStreamer.bot import StreamBot
from WebStreamer.utils import get_hash, get_name
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

@StreamBot.on_message(
    filters.private
    & (
        filters.document
        | filters.video
    ),
    group=4,
)
async def media_receive_handler(_, m: Message):
    log_msg = await m.forward(chat_id=Var.BIN_CHANNEL)
    new_url = "https://t.me/c/1844389952/"
    stream_link = f"{new_url}{log_msg.id} | {Var.URL}{log_msg.id}/{quote_plus(get_name(m))}"
    short_link = f"{new_url}{get_hash(log_msg)}{log_msg.id}"
    logging.info(f"Generated link: {stream_link} for {m.from_user.first_name}")
    rm = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Buka", url=stream_link)]]
    )
    if Var.FQDN == Var.BIND_ADDRESS:
        rm = None
    await m.reply_text(
        text="<code>{}</code>\n(<a href='{}'>Shortlink</a>)".format(
            stream_link, short_link
        ),
        quote=True,
        parse_mode=ParseMode.HTML,
        reply_markup=rm,
    )
