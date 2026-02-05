import os
import asyncio
import random
import sqlite3
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# ====== ТОКЕН ИЗ ПЕРЕМЕННОЙ ОКРУЖЕНИЯ ======
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("Не найден BOT_TOKEN в переменных окружения")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== НАСТРОЙКИ ======
START_BALANCE = 1000
BET_OPTIONS = [50, 100, 250]

# ====== КАРТЫ ======
RANKS = ["2","3","4","5","6","7","8","9","10","J","Q","K","A"]
SUITS = ["♠️","♥️","♦️","♣️"]

def random_card():
    return random.choice(RANKS), random.choice(SUITS)

def card_value(card):
    rank, _ = card
    if rank in ["J","Q","K"]:
        return 10
    if rank == "A":
        return 11
    return int(rank)

def hand_value(hand):
    value = sum(card_value(c) for c in hand)
    aces = sum(1 for c in hand if c[0] == "A")
    while value > 21 and aces:
        value -= 10
        aces -= 1
    return value

def render_hand(hand):
    return " ".join(f"{rank}{suit}" for rank, suit in hand)

# ====== БАЗА ======
conn = sqlite3.connect("database.db")
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    balance INTEGER,
    games INTEGER,
    wins INTEGER,
    losses INTEGER,
    pushes INTEGER,
    blackjacks INTEGER,
    max_balance INTEGER
)
""")
conn.commit()

# ====== СЛОВАРЬ ДЛЯ ИГРОКОВ ======
players = {}

def load_player(user_id):
    """Подгружает игрока из базы или создаёт нового"""
    if user_id in players:
        return players[user_id]

    cursor.execute(
        "SELECT balance, games, wins, losses, pushes, blackjacks, max_balance FROM users WHERE user_id=?",
        (user_id,)
    )
    row = cursor.fetchone()
    if row:
        balance, games, wins, losses, pushes, blackjacks, max_balance = row
        players[user_id] = {
            "balance": balance,
            "bet": None,
            "last_bet": None,
            "in_game": False,
            "player": [],
            "dealer": [],
            "stats": {
                "games": games,
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "blackjacks": blackjacks,
                "max_balance": max_balance
            }
        }
    else:
        players[user_id] = {
            "balance": START_BALANCE,
            "bet": None,
            "last_bet": None,
            "in_game": False,
            "player": [],
            "dealer": [],
            "stats": {
                "games": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "blackjacks": 0,
                "max_balance": START_BALANCE
            }
        }
        cursor.execute(
            "INSERT OR IGNORE INTO users VALUES (?,?,?,?,?,?,?,?)",
            (user_id, START_BALANCE, 0, 0, 0, 0, 0, START_BALANCE)
        )
        conn.commit()
    return players[user_id]

def save_player(user_id):
    """Сохраняет игрока в базу"""
    user = players[user_id]
    s = user["stats"]
    cursor.execute(
        """
        INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            user_id,
            user["balance"],
            s["games"],
            s["wins"],
            s["losses"],
            s["pushes"],
            s["blackjacks"],
            s["max_balance"]
        )
    )
    conn.commit()

# ====== КЛАВИАТУРЫ ======
def main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🃏 Играть", callback_data="play")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="show_stats")]
    ])

def bet_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 {b}", callback_data=f"bet_{b}") for b in BET_OPTIONS]
    ])

def repeat_bet_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Повторить ставку", callback_data="repeat_bet"),
            InlineKeyboardButton(text="✏️ Изменить ставку", callback_data="change_bet")
        ]
    ])

def game_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🖐 HIT", callback_data="hit"),
            InlineKeyboardButton(text="✋ STAND", callback_data="stand")
        ]
    ])

def stats_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Обратно к игре", callback_data="back_to_menu")]
    ])

# ====== /START ======
@dp.message(Command("start"))
async def start(message: types.Message):
    user = load_player(message.from_user.id)
    await message.answer(
        "🃏 *Blackjack*\n\n"
        "Классические правила.\n"
        "Blackjack платит 3:2.\n\n"
        f"💰 Баланс: {user['balance']}",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

# ====== ИГРА ======
@dp.callback_query(lambda c: c.data == "play")
async def play(call: CallbackQuery):
    user = load_player(call.from_user.id)
    if user["last_bet"]:
        text = (
            f"💰 Текущая ставка: {user['last_bet']}\n"
            f"💰 Баланс: {user['balance']}"
        )
        await call.message.edit_text(text, reply_markup=repeat_bet_keyboard())
    else:
        await call.message.edit_text(
            f"💰 Баланс: {user['balance']}\nВыбери ставку:",
            reply_markup=bet_keyboard()
        )

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def set_bet(call: CallbackQuery):
    bet = int(call.data.split("_")[1])
    user = load_player(call.from_user.id)
    if bet > user["balance"]:
        await call.answer("Недостаточно фишек 😬", show_alert=True)
        return
    user["bet"] = bet
    user["last_bet"] = bet
    await start_round(call)

@dp.callback_query(lambda c: c.data == "repeat_bet")
async def repeat_bet(call: CallbackQuery):
    user = load_player(call.from_user.id)
    user["bet"] = user["last_bet"]
    await start_round(call)

@dp.callback_query(lambda c: c.data == "change_bet")
async def change_bet(call: CallbackQuery):
    user = load_player(call.from_user.id)
    await call.message.edit_text(
        f"💰 Баланс: {user['balance']}\nВыбери новую ставку:",
        reply_markup=bet_keyboard()
    )

async def start_round(call: CallbackQuery):
    user = load_player(call.from_user.id)
    user["in_game"] = True
    user["player"] = [random_card(), random_card()]
    user["dealer"] = [random_card(), random_card()]

    text = (
        f"🧑 Ты: {render_hand(user['player'])} ({hand_value(user['player'])})\n"
        f"🤵 Дилер: {user['dealer'][0][0]}{user['dealer'][0][1]} ❓\n"
        f"💰 Баланс: {user['balance']}\n"
        f"💰 Ставка: {user['bet']}"
    )
    await call.message.edit_text(text, reply_markup=game_keyboard())

    if hand_value(user["player"]) == 21 and len(user["player"]) == 2:
        await finish_round(call, blackjack=True)

# ====== ХОДЫ ======
@dp.callback_query(lambda c: c.data == "hit")
async def hit(call: CallbackQuery):
    user = load_player(call.from_user.id)
    user["player"].append(random_card())
    if hand_value(user["player"]) > 21:
        await finish_round(call, lose=True)
    else:
        text = (
            f"🧑 Ты: {render_hand(user['player'])} ({hand_value(user['player'])})\n"
            f"🤵 Дилер: {user['dealer'][0][0]}{user['dealer'][0][1]} ❓\n"
            f"💰 Баланс: {user['balance']}\n"
            f"💰 Ставка: {user['bet']}"
        )
        await call.message.edit_text(text, reply_markup=game_keyboard())

@dp.callback_query(lambda c: c.data == "stand")
async def stand(call: CallbackQuery):
    user = load_player(call.from_user.id)
    while hand_value(user["dealer"]) < 17:
        user["dealer"].append(random_card())
    await finish_round(call)

# ====== КОНЕЦ РАУНДА ======
async def finish_round(call: CallbackQuery, blackjack=False, lose=False):
    user = load_player(call.from_user.id)
    bet = user["bet"]
    stats = user["stats"]
    player_val = hand_value(user["player"])
    dealer_val = hand_value(user["dealer"])
    stats["games"] += 1

    if blackjack:
        win = int(bet * 1.5)
        user["balance"] += win
        stats["wins"] += 1
        stats["blackjacks"] += 1
        result = f"🃏 BLACKJACK! Ты выиграл {win}"
    elif player_val > 21:
        user["balance"] -= bet
        stats["losses"] += 1
        result = "❌ Перебор! Ты проиграл"
    elif dealer_val > 21 or player_val > dealer_val:
        user["balance"] += bet
        stats["wins"] += 1
        result = f"✅ Ты выиграл {bet}"
    elif player_val < dealer_val:
        user["balance"] -= bet
        stats["losses"] += 1
        result = "❌ Ты проиграл"
    else:
        stats["pushes"] += 1
        result = "🤝 Ничья"

    stats["max_balance"] = max(stats["max_balance"], user["balance"])
    user["in_game"] = False
    user["bet"] = None

    save_player(call.from_user.id)

    text = (
        f"{result}\n\n"
        f"🧑 Ты: {render_hand(user['player'])} ({player_val})\n"
        f"🤵 Дилер: {render_hand(user['dealer'])} ({dealer_val})\n\n"
        f"💰 Баланс: {user['balance']}"
    )

    await call.message.edit_text(text, reply_markup=main_menu_keyboard())

# ====== СТАТИСТИКА ======
@dp.callback_query(lambda c: c.data == "show_stats")
async def show_stats(call: CallbackQuery):
    user = load_player(call.from_user.id)
    s = user["stats"]
    bal = user["balance"]
    await call.message.edit_text(
        "📊 *Твоя статистика*\n\n"
        f"🎲 Игр: {s['games']}\n"
        f"✅ Побед: {s['wins']}\n"
        f"❌ Поражений: {s['losses']}\n"
        f"🤝 Ничьих: {s['pushes']}\n"
        f"🃏 Blackjack: {s['blackjacks']}\n\n"
        f"💰 Баланс: {bal}\n"
        f"🏆 Максимум: {s['max_balance']}",
        parse_mode="Markdown",
        reply_markup=stats_keyboard()
    )

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(call: CallbackQuery):
    user = load_player(call.from_user.id)
    await call.message.edit_text(
        f"💰 Баланс: {user['balance']}",
        reply_markup=main_menu_keyboard()
    )

# ====== ЗАПУСК ======
async def main():
    print("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
