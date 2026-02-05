import os
import asyncio
import random
import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# ====== ТОКЕН И DATABASE_URL ======
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise ValueError("No BOT_TOKEN")
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== СОСТОЯНИЯ (FSM) ======
class GameStates(StatesGroup):
    waiting_for_bet = State()

# ====== НАСТРОЙКИ ======
START_BALANCE = 1000
BET_OPTIONS = [50, 100, 250]
RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
SUITS = ["♠️", "♥️", "♦️", "♣️"]

# Настройки колоды (Казино)
DECKS_COUNT = 5            # 5 колод
TOTAL_CARDS = 52 * DECKS_COUNT # 260 карт
RESHUFFLE_THRESHOLD = 60   # Перемешать, если осталось меньше 60 карт (подрезная карта)

# ====== АСИНХРОННАЯ БАЗА (asyncpg) ======
pool = None

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
            return {
                "balance": START_BALANCE,
                "stats": {"games":0, "wins":0, "losses":0, "pushes":0, "blackjacks":0, "max_balance":START_BALANCE}
            }
        
        return {
            "balance": row["balance"],
            "stats": {
                "games": row["games"], "wins": row["wins"], "losses": row["losses"],
                "pushes": row["pushes"], "blackjacks": row["blackjacks"], "max_balance": row["max_balance"]
            }
        }

async def update_player_db(user_id, balance, stats):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET 
                balance = $2, 
                games = $3, wins = $4, losses = $5, pushes = $6, blackjacks = $7, max_balance = $8
            WHERE user_id = $1
        """, user_id, balance, stats["games"], stats["wins"], stats["losses"], stats["pushes"], stats["blackjacks"], stats["max_balance"])

# ====== ЛОГИКА КОЛОДЫ (SHOE) ======
# user_id -> [список карт]
user_shoes = {}

def create_shoe():
    """Создает новую 'туфлю' из 5 колод и перемешивает"""
    base_deck = [(r, s) for r in RANKS for s in SUITS]
    shoe = base_deck * DECKS_COUNT
    random.shuffle(shoe)
    return shoe

def get_card(user_id):
    """Берет карту из колоды игрока. Если карт мало — мешает новую."""
    if user_id not in user_shoes:
        user_shoes[user_id] = create_shoe()
    
    shoe = user_shoes[user_id]
    shuffled_msg = None

    # Проверка "подрезной карты" (Penetration)
    if len(shoe) < RESHUFFLE_THRESHOLD:
        user_shoes[user_id] = create_shoe()
        shoe = user_shoes[user_id]
        shuffled_msg = "🔄 Колода перемешана"
    
    card = shoe.pop()
    return card, shuffled_msg

# ====== ЛОГИКА ИГРЫ ======
active_games = {} # user_id -> dict

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
    kb = [[InlineKeyboardButton(text=f"💰 {b}", callback_data=f"bet_{b}")] for b in BET_OPTIONS]
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data="custom_bet")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def game_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖐 HIT", callback_data="hit"),
         InlineKeyboardButton(text="✋ STAND", callback_data="stand")]
    ])

def game_over_kb(bet):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Играть еще", callback_data=f"play_again_{bet}")],
        [InlineKeyboardButton(text="💰 Изменить ставку", callback_data="play")]
    ])

# ====== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ЗАПУСКА ======
async def start_game_logic(user_id, bet, messageable):
    p = await get_player(user_id)
    
    if p['balance'] < bet:
        text = f"❌ Недостаточно фишек!\nТвой баланс: 🪙 {p['balance']}\nСтавка: {bet}"
        if isinstance(messageable, types.CallbackQuery):
            await messageable.answer("Недостаточно средств", show_alert=True)
            await messageable.message.edit_text(text, reply_markup=bet_kb())
        else:
            await messageable.answer(text, reply_markup=bet_kb())
        return

    # Раздача карт с учетом колоды
    c1, s1 = get_card(user_id)
    c2, s2 = get_card(user_id)
    d1, s3 = get_card(user_id)
    d2, s4 = get_card(user_id)
    
    # Собираем сообщения о перемешивании (если было)
    shuffles = [x for x in [s1, s2, s3, s4] if x]
    shuffle_note = f"\n\n_{shuffles[0]}_" if shuffles else ""

    active_games[user_id] = {
        "bet": bet,
        "player": [c1, c2],
        "dealer": [d1, d2]
    }
    
    g = active_games[user_id]
    txt = (f"💰 Ставка: {bet}\n"
           f"🤵 Дилер: {g['dealer'][0][0]}{g['dealer'][0][1]} ❓\n"
           f"🧑 Ты: {render_hand(g['player'])} ({hand_value(g['player'])})"
           f"{shuffle_note}")

    if isinstance(messageable, types.CallbackQuery):
        await messageable.message.edit_text(txt, reply_markup=game_kb(), parse_mode="Markdown")
    else:
        await messageable.answer(txt, reply_markup=game_kb(), parse_mode="Markdown")

    if hand_value(g['player']) == 21:
        await finish_game(user_id, messageable, blackjack=True)

# ====== ХЕНДЛЕРЫ ======
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    p = await get_player(message.from_user.id)
    await message.answer(
        f"🃏 *Blackjack*\n"
        f"Доброе пожаловать!\n"
        f"Классические выплаты 3:2 при BJ!\n\n"
        f"🪙 Баланс: {p['balance']}", 
        parse_mode="Markdown", reply_markup=main_menu_kb()
    )

@dp.callback_query(lambda c: c.data == "play")
async def cb_play(call: CallbackQuery):
    p = await get_player(call.from_user.id)
    await call.message.edit_text(f"🪙 Баланс: {p['balance']}\nСтавка:", reply_markup=bet_kb())

@dp.callback_query(lambda c: c.data == "custom_bet")
async def cb_custom_bet(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("✍️ Введи сумму ставки (целое число):")
    await state.set_state(GameStates.waiting_for_bet)

@dp.message(GameStates.waiting_for_bet)
async def process_custom_bet(message: types.Message, state: FSMContext):
    try:
        bet = int(message.text)
        if bet <= 0:
            await message.answer("Ставка должна быть больше 0. Попробуй снова:")
            return
    except ValueError:
        await message.answer("Пожалуйста, введи целое число:")
        return

    await state.clear()
    await start_game_logic(message.from_user.id, bet, message)

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def cb_bet(call: CallbackQuery):
    bet = int(call.data.split("_")[1])
    await start_game_logic(call.from_user.id, bet, call)

# Хендлер для кнопки "Играть еще"
@dp.callback_query(lambda c: c.data.startswith("play_again_"))
async def cb_play_again(call: CallbackQuery):
    try:
        bet = int(call.data.split("_")[2]) # play_again_100 -> берем 100
        await start_game_logic(call.from_user.id, bet, call)
    except (IndexError, ValueError):
        await call.answer("Ошибка повтора ставки", show_alert=True)

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
    await call.message.edit_text(f"🪙 Баланс: {p['balance']}", reply_markup=main_menu_kb())

@dp.callback_query(lambda c: c.data == "hit")
async def cb_hit(call: CallbackQuery):
    uid = call.from_user.id
    if uid not in active_games: return
    g = active_games[uid]
    
    # Берем карту из колоды
    new_card, shuffle_msg = get_card(uid)
    g['player'].append(new_card)
    
    shuffle_note = f"\n\n_{shuffle_msg}_" if shuffle_msg else ""
    val = hand_value(g['player'])
    
    if val > 21:
        await finish_game(uid, call, lose=True)
    else:
        txt = (f"💰 Ставка: {g['bet']}\n"
               f"🤵 Дилер: {g['dealer'][0][0]}{g['dealer'][0][1]} ❓\n"
               f"🧑 Ты: {render_hand(g['player'])} ({val})"
               f"{shuffle_note}")
        await call.message.edit_text(txt, reply_markup=game_kb(), parse_mode="Markdown")

@dp.callback_query(lambda c: c.data == "stand")
async def cb_stand(call: CallbackQuery):
    uid = call.from_user.id
    if uid not in active_games: return
    g = active_games[uid]
    
    # Дилер добирает карты из колоды
    shuffle_happened = False
    while hand_value(g['dealer']) < 17:
        card, s_msg = get_card(uid)
        g['dealer'].append(card)
        if s_msg: shuffle_happened = True
    
    await finish_game(uid, call, shuffle_alert=shuffle_happened)

async def finish_game(user_id, messageable, blackjack=False, lose=False, shuffle_alert=False):
    if user_id not in active_games: return
    g = active_games.pop(user_id)
    p = await get_player(user_id)
    
    bet = g['bet']
    p_val = hand_value(g['player'])
    d_val = hand_value(g['dealer'])
    
    win_amount = 0
    res = "Ничья"
    
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

    new_bal = p['balance'] + win_amount
    p['stats']['games'] += 1
    p['stats']['max_balance'] = max(p['stats']['max_balance'], new_bal)
    
    await update_player_db(user_id, new_bal, p['stats'])
    
    shuffle_note = "\n\n_🔄 Колода перемешана_" if shuffle_alert else ""

    txt = (
        f"{res} ({win_amount:+})\n\n"
        f"🤵 Дилер: {render_hand(g['dealer'])} ({d_val})\n"
        f"🧑 Ты: {render_hand(g['player'])} ({p_val})\n\n"
        f"🪙 Баланс: {new_bal}"
        f"{shuffle_note}"
    )
    
    if isinstance(messageable, types.CallbackQuery):
        await messageable.message.edit_text(txt, reply_markup=game_over_kb(bet), parse_mode="Markdown")
    else:
        await messageable.answer(txt, reply_markup=game_over_kb(bet), parse_mode="Markdown")

# ====== ЗАПУСК ======
async def main():
    await init_db()
    print("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
