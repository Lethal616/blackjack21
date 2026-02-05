import os
import asyncio
import random
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ====== ТОКЕН И DATABASE_URL ======
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise ValueError("No BOT_TOKEN")
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== НАСТРОЙКИ ======
START_BALANCE = 1000
BET_OPTIONS = [50, 100, 250]
RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
SUITS = ["♠️", "♥️", "♦️", "♣️"]

# ====== АСИНХРОННАЯ БАЗА (asyncpg) ======
pool = None  # Пул соединений

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance INTEGER DEFAULT 1000,
                games INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                pushes INTEGER DEFAULT 0,
                blackjacks INTEGER DEFAULT 0,
                max_balance INTEGER DEFAULT 1000
            )
        """)
        print("Database initialized")

async def get_player(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        
        if not row:
            await conn.execute(
                "INSERT INTO users (user_id, balance, max_balance) VALUES ($1, $2, $2) ON CONFLICT DO NOTHING",
                user_id, START_BALANCE
            )
            # Возвращаем дефолт
            return {
                "balance": START_BALANCE,
                "stats": {"games":0, "wins":0, "losses":0, "pushes":0, "blackjacks":0, "max_balance":START_BALANCE},
                "bet": None, "last_bet": None, "in_game": False, "player": [], "dealer": []
            }
        
        return {
            "balance": row["balance"],
            "stats": {
                "games": row["games"], "wins": row["wins"], "losses": row["losses"],
                "pushes": row["pushes"], "blackjacks": row["blackjacks"], "max_balance": row["max_balance"]
            },
            "bet": None, "last_bet": None, "in_game": False, "player": [], "dealer": []
        }

async def update_player_db(user_id, balance, stats):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET 
                balance = $2, 
                games = $3, wins = $4, losses = $5, pushes = $6, blackjacks = $7, max_balance = $8
            WHERE user_id = $1
        """, user_id, balance, stats["games"], stats["wins"], stats["losses"], stats["pushes"], stats["blackjacks"], stats["max_balance"])

# ====== ЛОГИКА ИГРЫ (InMemory кеш для активной сессии) ======
active_games = {} # user_id -> dict

def random_card():
    return random.choice(RANKS), random.choice(SUITS)

def card_value(card):
    rank, _ = card
    if rank in ["J","Q","K"]: return 10
    if rank == "A": return 11
    return int(rank)

def hand_value(hand):
    val = sum(card_value(c) for c in hand)
    aces = sum(1 for c in hand if c[0]=="A")
    while val > 21 and aces:
        val -= 10
        aces -= 1
    return val

def render_hand(hand):
    return " ".join(f"{r}{s}" for r, s in hand)

# ====== КЛАВИАТУРЫ ======
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🃏 Играть", callback_data="play"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])

def bet_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 {b}", callback_data=f"bet_{b}") for b in BET_OPTIONS]
    ])

def game_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖐 HIT", callback_data="hit"),
         InlineKeyboardButton(text="✋ STAND", callback_data="stand")]
    ])

# ====== ХЕНДЛЕРЫ ======
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    p = await get_player(message.from_user.id)
    await message.answer(
        f"🃏 *Blackjack*\nБаланс: {p['balance']}", 
        parse_mode="Markdown", reply_markup=main_menu_kb()
    )

@dp.callback_query(lambda c: c.data == "play")
async def cb_play(call: CallbackQuery):
    p = await get_player(call.from_user.id)
    await call.message.edit_text(f"Баланс: {p['balance']}\nСтавка:", reply_markup=bet_kb())

@dp.callback_query(lambda c: c.data == "stats")
async def cb_stats(call: CallbackQuery):
    p = await get_player(call.from_user.id)
    s = p['stats']
    await call.message.edit_text(
        f"📊 *Статистика*\nИгр: {s['games']}\nПобед: {s['wins']}\nМакс: {s['max_balance']}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="menu")]])
    )

@dp.callback_query(lambda c: c.data == "menu")
async def cb_menu(call: CallbackQuery):
    p = await get_player(call.from_user.id)
    await call.message.edit_text(f"Баланс: {p['balance']}", reply_markup=main_menu_kb())

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def cb_bet(call: CallbackQuery):
    bet = int(call.data.split("_")[1])
    uid = call.from_user.id
    p = await get_player(uid)
    
    if p['balance'] < bet:
        return await call.answer("Мало фишек!", show_alert=True)
    
    # Начинаем игру (в памяти)
    active_games[uid] = {
        "bet": bet,
        "player": [random_card(), random_card()],
        "dealer": [random_card(), random_card()]
    }
    
    g = active_games[uid]
    txt = f"💰 Ставка: {bet}\n🤵 Дилер: {g['dealer'][0][0]}{g['dealer'][0][1]} ❓\n🧑 Ты: {render_hand(g['player'])} ({hand_value(g['player'])})"
    await call.message.edit_text(txt, reply_markup=game_kb())

    if hand_value(g['player']) == 21:
        await finish_game(call, blackjack=True)

@dp.callback_query(lambda c: c.data == "hit")
async def cb_hit(call: CallbackQuery):
    uid = call.from_user.id
    if uid not in active_games: return
    g = active_games[uid]
    g['player'].append(random_card())
    
    val = hand_value(g['player'])
    if val > 21:
        await finish_game(call, lose=True)
    else:
        txt = f"💰 Ставка: {g['bet']}\n🤵 Дилер: {g['dealer'][0][0]}{g['dealer'][0][1]} ❓\n🧑 Ты: {render_hand(g['player'])} ({val})"
        await call.message.edit_text(txt, reply_markup=game_kb())

@dp.callback_query(lambda c: c.data == "stand")
async def cb_stand(call: CallbackQuery):
    uid = call.from_user.id
    if uid not in active_games: return
    g = active_games[uid]
    
    while hand_value(g['dealer']) < 17:
        g['dealer'].append(random_card())
    
    await finish_game(call)

async def finish_game(call, blackjack=False, lose=False):
    uid = call.from_user.id
    g = active_games.pop(uid)
    p = await get_player(uid) # свежие данные из БД
    
    bet = g['bet']
    p_val = hand_value(g['player'])
    d_val = hand_value(g['dealer'])
    
    win_amount = 0
    res = "Ничья"
    
    # Логика
    if lose or (not blackjack and p_val > 21):
        res = "❌ Перебор/Проигрыш"
        win_amount = -bet
        p['stats']['losses'] += 1
    elif blackjack:
        res = "🃏 BLACKJACK!"
        win_amount = int(bet * 1.5)
        p['stats']['wins'] += 1
        p['stats']['blackjacks'] += 1
    elif d_val > 21 or p_val > d_val:
        res = "✅ Победа!"
        win_amount = bet
        p['stats']['wins'] += 1
    elif p_val < d_val:
        res = "❌ Дилер выиграл"
        win_amount = -bet
        p['stats']['losses'] += 1
    else:
        res = "🤝 Ничья"
        p['stats']['pushes'] += 1

    # Обновляем баланс
    new_bal = p['balance'] + win_amount
    p['stats']['games'] += 1
    p['stats']['max_balance'] = max(p['stats']['max_balance'], new_bal)
    
    # Сохраняем в БД
    await update_player_db(uid, new_bal, p['stats'])
    
    txt = (
        f"{res} ({win_amount:+})\n"
        f"🧑 {render_hand(g['player'])} ({p_val})\n"
        f"🤵 {render_hand(g['dealer'])} ({d_val})\n"
        f"💰 Баланс: {new_bal}"
    )
    await call.message.edit_text(txt, reply_markup=main_menu_kb())

# ====== ЗАПУСК ======
async def main():
    await init_db() # Подключение к БД
    print("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
