import logging
import asyncio
import sqlite3
import uuid
import os
import random
import json
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from dotenv import load_dotenv
from aiohttp import web  # Добавили для Render

# --- КОНФИГУРАЦИЯ ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))

logging.basicConfig(level=logging.INFO)

# --- GOOGLE SHEETS API ---
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # На Render файл может лежать в другой папке, проверяем это
    filename = "service_account.json"
    if not os.path.exists(filename):
        logging.error(f"Файл {filename} не найден! Проверьте Secret Files на Render.")
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(filename, scope)
    return gspread.authorize(creds)

def get_prizes_from_sheet():
    client = get_gspread_client()
    sheet = client.open_by_key(SHEET_ID).worksheet("Prizes")
    all_records = sheet.get_all_records()
    
    available_prizes = []
    for idx, item in enumerate(all_records, start=2):
        try:
            limit = int(item['Лимит'])
            issued = int(item['Выдано'])
            if limit - issued > 0:
                item['row_idx'] = idx 
                available_prizes.append(item)
        except ValueError:
            continue
    return available_prizes

def record_winner(user: types.User, prize: dict):
    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    ws_prizes = sh.worksheet("Prizes")
    ws_prizes.update_cell(prize['row_idx'], 4, int(prize['Выдано']) + 1)
    
    ws_winners = sh.worksheet("Winners")
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        user.id,
        f"@{user.username}" if user.username else "NoUsername",
        prize['Название приза']
    ]
    ws_winners.append_row(row)

# --- SQLITE DATABASE (LOCAL) ---
DB_NAME = "roulette.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tokens (token TEXT PRIMARY KEY, status TEXT DEFAULT 'active')")
    conn.commit()
    conn.close()

def add_tokens_batch(tokens):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.executemany("INSERT OR IGNORE INTO tokens (token, status) VALUES (?, ?)", [(t, 'active') for t in tokens])
    conn.commit()
    conn.close()

def check_token_status(token):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT status FROM tokens WHERE token = ?", (token,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def mark_token_used(token):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("UPDATE tokens SET status = 'used' WHERE token = ?", (token,))
    conn.commit()
    conn.close()

# --- DUMMY WEB SERVER (Для Render Web Service) ---
async def health_check(request):
    return web.Response(text="Bot is running OK!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    # Render передает порт через переменную окружения PORT
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"Web server started on port {port}")

# --- БОТ ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

@dp.message_handler(commands=['generate'], user_id=ADMIN_IDS)
async def cmd_generate(message: types.Message):
    try:
        count = int(message.get_args())
    except (ValueError, TypeError):
        await message.reply("Использование: /generate <N>")
        return
    new_tokens = [str(uuid.uuid4())[:8] for _ in range(count)]
    add_tokens_batch(new_tokens)
    bot_username = (await bot.get_me()).username
    lines = [f"https://t.me/{bot_username}?start={t}" for t in new_tokens]
    with open("links.txt", "w") as f: f.write("\n".join(lines))
    await message.reply_document(open("links.txt", "rb"), caption=f"Сгенерировано {count}.")
    os.remove("links.txt")

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    token = message.get_args()
    if not token:
        await message.answer("Нужна ссылка.")
        return
    status = check_token_status(token)
    if status == 'active':
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🎰 Испытать удачу! 🎰", callback_data=f"spin:{token}"))
        await message.answer("Добро пожаловать!", reply_markup=markup)
    else:
        await message.answer("Ссылка недействительна или уже использована.")

@dp.callback_query_handler(lambda c: c.data.startswith('spin:'))
async def process_spin(callback_query: types.CallbackQuery):
    token = callback_query.data.split(":")[1]
    if check_token_status(token) != 'active':
        await bot.answer_callback_query(callback_query.id, "Уже неактивно.")
        await callback_query.message.delete()
        return
    await callback_query.message.edit_reply_markup(reply_markup=None)
    await bot.send_dice(callback_query.from_user.id, emoji='🎰')
    await asyncio.sleep(2.5)
    try:
        prizes = get_prizes_from_sheet()
        if not prizes:
             await bot.send_message(callback_query.from_user.id, "Призы закончились! 😔")
             mark_token_used(token)
             return
        won_prize = random.choice(prizes)
        record_winner(callback_query.from_user, won_prize)
        mark_token_used(token)
        await bot.send_message(callback_query.from_user.id, f"🎉 Приз: <b>{won_prize['Название приза']}</b>", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error: {e}")
        await bot.send_message(callback_query.from_user.id, "Ошибка.")

async def on_startup(dp):
    init_db()
    # Запускаем веб-сервер вместе с ботом
    await start_web_server()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    