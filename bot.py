import logging
import asyncio
import uuid
import os
import random
import json
from datetime import datetime

import gspread
import aiohttp
from aiohttp import web
from oauth2client.service_account import ServiceAccountCredentials
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor
from aiogram.utils.exceptions import MessageNotModified
from dotenv import load_dotenv

# --- КОНФИГУРАЦИЯ ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))

logging.basicConfig(level=logging.INFO)

# --- GOOGLE SHEETS API ---
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    filename = "service_account.json"
    if not os.path.exists(filename):
        logging.error(f"Файл {filename} не найден! Проверьте Secret Files на Render.")
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(filename, scope)
    return gspread.authorize(creds)

def get_prizes_from_sheet():
    """Получает список доступных призов."""
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
    """Записывает победителя в таблицу."""
    client = get_gspread_client()
    sh = client.open_by_key(SHEET_ID)
    
    # 1. Обновляем счетчик призов
    ws_prizes = sh.worksheet("Prizes")
    ws_prizes.update_cell(prize['row_idx'], 4, int(prize['Выдано']) + 1)
    
    # 2. Добавляем запись в Winners
    ws_winners = sh.worksheet("Winners")
    username = f"@{user.username}" if user.username else "NoUsername"
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        user.id,
        username,
        prize['Название приза']
    ]
    ws_winners.append_row(row)
    
    # Лог для контроля
    logging.info(f"Пользователь ({username}, {user.id}) выиграл {prize['Название приза']}")

# --- TOKENS IN GOOGLE SHEETS ---
def add_tokens_to_sheet(tokens):
    """Добавляет пачку новых токенов в таблицу."""
    client = get_gspread_client()
    ws = client.open_by_key(SHEET_ID).worksheet("Tokens")
    data = [[t, 'active'] for t in tokens]
    ws.append_rows(data)

def check_token_status_sheet(token):
    """Проверяет статус токена в таблице."""
    client = get_gspread_client()
    ws = client.open_by_key(SHEET_ID).worksheet("Tokens")
    try:
        cell = ws.find(token)
        if cell:
            status = ws.cell(cell.row, cell.col + 1).value
            return status, cell.row, cell.col + 1
    except Exception as e:
        logging.error(f"Ошибка поиска токена: {e}")
    return None, None, None

def mark_token_used_sheet(row, col):
    """Помечает токен как used."""
    client = get_gspread_client()
    ws = client.open_by_key(SHEET_ID).worksheet("Tokens")
    ws.update_cell(row, col, 'used')

# --- WEB SERVER & KEEP ALIVE ---
web_runner = None

async def health_check(request):
    return web.Response(text="Bot is running OK!")

async def start_web_server():
    global web_runner
    app = web.Application()
    app.router.add_get('/', health_check)
    web_runner = web.AppRunner(app)
    await web_runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(web_runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"Web server started on port {port}")

async def keep_alive():
    """Пингует сам себя каждые 9 минут."""
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    logging.info(f"Запущен Keep-Alive пингер для {url}")
    while True:
        await asyncio.sleep(9 * 60)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    logging.info(f"Keep-alive status: {resp.status}")
        except Exception as e:
            logging.error(f"Keep-alive ошибка: {e}")

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
    try:
        await message.reply("Сохраняю токены в Google Таблицу... Ждите.")
        add_tokens_to_sheet(new_tokens)
    except Exception as e:
        logging.error(f"Ошибка записи: {e}")
        await message.reply("Ошибка записи в таблицу.")
        return

    logging.info(f"сгенерированы {count} штук ссылок")
    bot_username = (await bot.get_me()).username
    lines = [f"https://t.me/{bot_username}?start={t}" for t in new_tokens]
    
    with open("links.txt", "w") as f: f.write("\n".join(lines))
    await message.reply_document(open("links.txt", "rb"), caption=f"Готово: {count} ссылок.")
    os.remove("links.txt")

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    token = message.get_args()
    if not token:
        await message.answer("Для участия нужна ссылка.")
        return

    status, _, _ = check_token_status_sheet(token)
    if status == 'active':
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🚀 Запустить систему розыгрыша", callback_data=f"step1:{token}"))
        await message.answer("👋 Привет! Ты в шаге от приза.\n\nСистема готова. Начинаем?", reply_markup=markup)
    elif status == 'used':
        await message.answer("Эта ссылка уже была использована.")
    else:
        await message.answer("Неверная ссылка.")

@dp.callback_query_handler(lambda c: c.data.startswith('step1:'))
async def process_step_1(callback_query: types.CallbackQuery):
    token = callback_query.data.split(":")[1]
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⚡ Зарядить на удачу ⚡", callback_data=f"step2:{token}"))
    await callback_query.message.edit_text(
        "📡 Связь с космосом установлена...\n🔄 Калибровка удачи... [████░░]\n🔎 Поиск лучших призов...", 
        reply_markup=markup
    )

@dp.callback_query_handler(lambda c: c.data.startswith('step2:'))
async def process_step_2(callback_query: types.CallbackQuery):
    token = callback_query.data.split(":")[1]
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🎰 КРУТИТЬ РУЛЕТКУ! 🎰", callback_data=f"spin:{token}"))
    await callback_query.message.edit_text(
        "🔋 Энергия: 100%\n🍀 Удача: МАКСИМУМ\n🔥 Система готова к выдаче приза!", 
        reply_markup=markup
    )

@dp.callback_query_handler(lambda c: c.data.startswith('spin:'))
async def process_spin(callback_query: types.CallbackQuery):
    token = callback_query.data.split(":")[1]
    
    status, row_idx, col_idx = check_token_status_sheet(token)
    if status != 'active':
        await bot.answer_callback_query(callback_query.id, "Ссылка неактивна.")
        await callback_query.message.delete()
        return

    try:
        await callback_query.message.edit_reply_markup(reply_markup=None)
    except MessageNotModified:
        pass 
    
    await bot.send_dice(callback_query.from_user.id, emoji='🎰')
    await asyncio.sleep(2.5)
    
    try:
        prizes = get_prizes_from_sheet()
        if not prizes:
             await bot.send_message(callback_query.from_user.id, "Призы закончились! 😔")
             if row_idx and col_idx:
                 mark_token_used_sheet(row_idx, col_idx)
             return

        won_prize = random.choice(prizes)
        record_winner(callback_query.from_user, won_prize)
        
        if row_idx and col_idx:
            mark_token_used_sheet(row_idx, col_idx)
        
        await bot.send_message(
            callback_query.from_user.id, 
            f"🎇🎇🎇 <b>БА-БАХ! ЕСТЬ КОНТАКТ!</b> 🎇🎇🎇\n\n"
            f"🎁 Ваш приз: <b>{won_prize['Название приза']}</b>\n\n"
            f"🥳 Поздравляем с победой!", 
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Error process_spin: {e}")
        await bot.send_message(callback_query.from_user.id, "Произошла ошибка при обработке.")

async def on_startup(dp):
    asyncio.create_task(keep_alive())
    # 1. Запускаем веб-сервер, чтобы Render увидел, что мы живы
    await start_web_server()
    
    # 2. Удаляем вебхук на всякий случай
    await bot.delete_webhook(drop_pending_updates=True)

    # 3. ХИТРОСТЬ: Ждем 15 секунд, чтобы старый бот успел умереть, 
    # пока Render переключает трафик.
    logging.info("⏳ Пауза 15 сек перед запуском Polling (ждем завершения старой версии)...")
    await asyncio.sleep(15)
    logging.info("🚀 Старт Polling!")

async def on_shutdown(dp):
    logging.warning('Shutting down bot...')
    if web_runner:
        await web_runner.cleanup()
    await bot.close()
    logging.warning('Bot stopped')

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
    