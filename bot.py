import os
import asyncio
import random
import asyncpg
import uuid
import time
import json 
from datetime import datetime, timedelta, timezone, time as dt_time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# ====== КОНФИГУРАЦИЯ ======
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN or not DATABASE_URL:
    raise ValueError("No TOKEN or DATABASE_URL provided")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ====== КОНСТАНТЫ ======
RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
SUITS = ["♠️", "♥️", "♦️", "♣️"]
DECKS_COUNT = 5
TOTAL_CARDS = 52 * DECKS_COUNT
RESHUFFLE_THRESHOLD = 60
BET_OPTIONS = [50, 100, 250]
MAX_PLAYERS = 3
TURN_TIMEOUT = 30 

# ====== БАЗА ДАННЫХ ======
pool = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        # Таблица пользователей
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT, 
                balance INTEGER DEFAULT 1000,
                games INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                pushes INTEGER DEFAULT 0,
                blackjacks INTEGER DEFAULT 0,
                max_balance INTEGER DEFAULT 1000,
                max_win INTEGER DEFAULT 0
            )
        """)
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        except: pass

        # Таблица логов игр
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS game_logs (
                id SERIAL PRIMARY KEY,
                table_id TEXT,
                user_id BIGINT,
                username TEXT, 
                bet INTEGER,
                result TEXT, 
                win_amount INTEGER, 
                player_hand TEXT,
                dealer_hand TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        try:
            await conn.execute("ALTER TABLE game_logs ADD COLUMN IF NOT EXISTS username TEXT")
        except: pass
        
        # Таблица логов чата
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_logs (
                id SERIAL PRIMARY KEY,
                table_id TEXT,
                user_id BIGINT,
                username TEXT,
                message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

    print("Database initialized with logs and usernames")

async def get_player_data(user_id, username=None):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        
        if not row:
            await conn.execute(
                "INSERT INTO users (user_id, username, balance, max_balance, max_win) VALUES ($1, $2, $3, $3, 0) ON CONFLICT (user_id) DO NOTHING",
                user_id, username, 1000
            )
            return {"balance": 1000, "username": username, "stats": {"games":0, "wins":0, "losses":0, "pushes":0, "blackjacks":0, "max_balance":1000, "max_win":0}}
        
        if username and row['username'] != username:
             await conn.execute("UPDATE users SET username = $2 WHERE user_id = $1", user_id, username)
        
        return {
            "balance": row["balance"],
            "username": row["username"], 
            "stats": {
                "games": row["games"], "wins": row["wins"], "losses": row["losses"],
                "pushes": row["pushes"], "blackjacks": row["blackjacks"],
                "max_balance": row["max_balance"], "max_win": row.get("max_win", 0) or 0
            }
        }

async def update_player_stats(user_id, balance, stats):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET 
                balance = $2, games = $3, wins = $4, losses = $5, 
                pushes = $6, blackjacks = $7, max_balance = $8, max_win = $9
            WHERE user_id = $1
        """, user_id, balance, stats["games"], stats["wins"], stats["losses"], 
           stats["pushes"], stats["blackjacks"], stats["max_balance"], stats["max_win"])

async def log_game(table_id, user_id, username, bet, result, win_amount, p_hand, d_hand):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO game_logs (table_id, user_id, username, bet, result, win_amount, player_hand, dealer_hand)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, table_id, user_id, username, bet, result, win_amount, str(p_hand), str(d_hand))

async def log_chat(table_id, user_id, username, message):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_logs (table_id, user_id, username, message)
            VALUES ($1, $2, $3, $4)
        """, table_id, user_id, username, message)

# ====== ЛОГИКА ИГРЫ (КЛАССЫ) ======

class CardSystem:
    def __init__(self):
        self.shoe = []
        self.create_shoe()

    def create_shoe(self):
        base_deck = [(r, s) for r in RANKS for s in SUITS]
        self.shoe = base_deck * DECKS_COUNT
        random.shuffle(self.shoe)

    def get_card(self):
        if len(self.shoe) < RESHUFFLE_THRESHOLD:
            self.create_shoe()
            return self.shoe.pop(), True 
        return self.shoe.pop(), False

    def get_visual_bar(self):
        percent = len(self.shoe) / TOTAL_CARDS
        blocks = int(percent * 8)
        bar = "▰" * blocks + "▱" * (8 - blocks)
        return f"{bar} {int(percent * 100)}%"

class TablePlayer:
    def __init__(self, user_id, name, bet, start_balance):
        self.user_id = user_id
        self.name = name
        self.bet = bet
        self.original_bet = bet
        self.hand = []
        self.status = "waiting" # waiting, playing, stand, bust, blackjack
        self.is_ready = False 
        self.message_id = None 
        self.start_balance = start_balance
        self.last_action = None 

    @property
    def value(self):
        val = sum(10 if c[0] in "JQK" else 11 if c[0] == "A" else int(c[0]) for c in self.hand)
        aces = sum(1 for c in self.hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

    def render_hand(self):
        if not self.hand: return ""
        return " ".join(f"`{r}{s}`" for r, s in self.hand)

class GameTable:
    def __init__(self, table_id, is_public=False, owner_id=None):
        self.id = table_id
        self.is_public = is_public
        self.owner_id = owner_id
        self.players = [] 
        self.dealer_hand = []
        self.deck = CardSystem()
        self.state = "waiting" # waiting, player_turn, dealer_turn, finished
        self.current_player_index = 0
        self.shuffle_alert = False
        self.last_action_time = time.time()
        self.chat_history = [] 

    def add_player(self, user_id, name, bet, current_balance):
        player = TablePlayer(user_id, name, bet, start_balance=current_balance)
        self.players.append(player)
        self.update_activity()
        return player

    def remove_player(self, user_id):
        self.players = [p for p in self.players if p.user_id != user_id]
        if user_id == self.owner_id:
            if self.players:
                self.owner_id = self.players[0].user_id
            else:
                self.owner_id = None 
        self.update_activity()

    def get_player(self, user_id):
        for p in self.players:
            if p.user_id == user_id:
                return p
        return None
    
    def add_chat_message(self, name, text):
        clean_text = text[:30] 
        self.chat_history.append(f"{name}: {clean_text}")
        if len(self.chat_history) > 5: 
            self.chat_history.pop(0)
    
    def check_all_ready(self):
        if not self.players: return False
        return all(p.is_ready for p in self.players)

    def reset_round(self):
        self.state = "waiting"
        self.dealer_hand = []
        for p in self.players:
            p.hand = []
            p.is_ready = False 
            p.status = "waiting"
            p.bet = p.original_bet 
            p.last_action = None 
        self.update_activity()

    def update_activity(self):
        self.last_action_time = time.time()

    def start_game(self):
        self.dealer_hand = []
        self.shuffle_alert = False
        
        c, s = self.deck.get_card()
        if s: self.shuffle_alert = True
        self.dealer_hand.append(c)
        
        c, s = self.deck.get_card()
        if s: self.shuffle_alert = True
        self.dealer_hand.append(c)

        for p in self.players:
            p.bet = p.original_bet 
            p.hand = []
            p.status = "playing"
            p.last_action = None
            c1, s1 = self.deck.get_card()
            c2, s2 = self.deck.get_card()
            p.hand = [c1, c2]
            if s1 or s2: self.shuffle_alert = True
            
            if p.value == 21:
                p.status = "blackjack"
        
        self.state = "player_turn"
        self.current_player_index = 0
        self.process_turns() 

    def process_turns(self):
        self.update_activity() 
        while self.current_player_index < len(self.players):
            p = self.players[self.current_player_index]
            if p.status == "playing":
                return 
            self.current_player_index += 1
        
        self.state = "dealer_turn"
        self.play_dealer()

    def play_dealer(self):
        val = self._hand_value(self.dealer_hand)
        while val < 17:
            c, s = self.deck.get_card()
            if s: self.shuffle_alert = True
            self.dealer_hand.append(c)
            val = self._hand_value(self.dealer_hand)
        self.state = "finished"

    def _hand_value(self, hand):
        val = sum(10 if c[0] in "JQK" else 11 if c[0] == "A" else int(c[0]) for c in hand)
        aces = sum(1 for c in hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

tables = {} 

def leave_all_tables(user_id, exclude_tid=None):
    for tid in list(tables.keys()):
        if tid == exclude_tid: continue
        table = tables.get(tid)
        if table and table.get_player(user_id):
            table.remove_player(user_id)
            if not table.players:
                del tables[tid]

# ====== ФОНОВАЯ ЗАДАЧА ======
async def check_timeouts_loop():
    while True:
        await asyncio.sleep(5) 
        now = time.time()
        
        for table in list(tables.values()):
            if table.state == "player_turn":
                if now - table.last_action_time > TURN_TIMEOUT:
                    try:
                        current_p = table.players[table.current_player_index]
                        current_p.status = "stand" 
                        current_p.last_action = "stand" 
                        
                        table.process_turns()
                        
                        if table.state == "finished":
                            await finalize_game_db(table)
                        
                        await update_table_messages(table.id)
                        
                        try: await bot.send_message(current_p.user_id, "⏳ Время хода вышло! Авто-Stand.")
                        except: pass
                        
                    except IndexError:
                        pass 

# ====== ВИЗУАЛИЗАЦИЯ ======

def render_lobby(table: GameTable):
    txt = f"🎰 *BLACKJACK TABLE #{table.id}*\n"
    txt += f"───────────────\n"
    
    for i, p in enumerate(table.players, 1):
        role = "👑" if p.user_id == table.owner_id else "👤"
        status = "✅ ГОТОВ" if p.is_ready else "⏳ НЕ ГОТОВ"
        txt += f"{status} {role} *{p.name}* — {p.bet} 🪙\n"
    
    txt += f"───────────────\n"
    txt += f"👥 Мест: {len(table.players)}/{MAX_PLAYERS}\n"
    
    if table.chat_history:
        txt += "\n💬 *LIVE CHAT:*\n" + "\n".join([f"▫️ {msg}" for msg in table.chat_history])
    else:
        txt += "\n💬 (Напишите сообщение...)"

    return txt

def get_lobby_kb(table: GameTable, user_id):
    kb = []
    p = table.get_player(user_id)
    
    if not p.is_ready:
        kb.append([InlineKeyboardButton(text="✅ Я ГОТОВ", callback_data=f"ready_{table.id}")])
        kb.append([InlineKeyboardButton(text="💰 Изм. ставку", callback_data=f"chbet_lobby_{table.id}")])
    
    kb.append([InlineKeyboardButton(text="🚪 Выйти", callback_data=f"leave_lobby_{table.id}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def render_table_for_player(table: GameTable, player: TablePlayer, bot: Bot):
    if table.state == "finished":
        d_val = table._hand_value(table.dealer_hand)
        d_cards = " ".join(f"`{r}{s}`" for r,s in table.dealer_hand)
        dealer_section = (
            f"🤵 *DEALER*\n"
            f"{d_cards} ➡️ *{d_val}*\n"
        )
    else:
        visible = table.dealer_hand[0]
        vis_val = table._hand_value([visible])
        d_cards = f"`{visible[0]}{visible[1]}` `??`"
        dealer_section = (
            f"🤵 *DEALER*\n"
            f"{d_cards} ➡️ *{vis_val}*\n"
        )

    players_section = ""
    for p in table.players:
        status_marker = "💤"
        status_text = ""
        action_trail = "" 

        if p.last_action == "hit": action_trail = " (🤏 HIT)"
        elif p.last_action == "stand": action_trail = " (✋ STAND)"
        elif p.last_action == "double": action_trail = " (2️⃣ DOUBLE)"

        if table.state == "player_turn":
            if table.players[table.current_player_index] == p:
                status_marker = "⏳" 
                action_trail = " (🤔 ДУМАЕТ...)" 
            elif table.players.index(p) > table.current_player_index:
                status_marker = "💤" 
                action_trail = " (💤 ЖДЕТ)"
            else:
                status_marker = "✅" 
        elif table.state == "finished":
             d_val = table._hand_value(table.dealer_hand)
             if p.status == "bust": 
                 status_marker = "💀"
                 status_text = "   _❌ ПЕРЕБОР_"
             elif p.status == "blackjack": 
                 status_marker = "🔥"
                 status_text = f"   _*🃏 BLACKJACK! (+{int(p.bet * 1.5)})*_"
             elif d_val > 21 or (p.value <= 21 and p.value > d_val): 
                 status_marker = "🏆"
                 status_text = f"   _*✅ ПОБЕДА (+{p.bet})*_"
             elif p.value == d_val: 
                 status_marker = "🤝"
                 status_text = "   _🤝 НИЧЬЯ_"
             else: 
                 status_marker = "❌"
                 status_text = "   _❌ ПРОИГРЫШ_"

        is_me = " (Вы)" if p.user_id == player.user_id else ""
        name_line = f"{status_marker} *{p.name}*{is_me}{action_trail} • {p.bet}💰"
        cards_line = f"   {p.render_hand()}  ➡️ *{p.value}*"
        
        full_status_line = f"\n{status_text}" if status_text else ""
        players_section += f"{name_line}\n{cards_line}{full_status_line}\n\n"

    p_data = await get_player_data(player.user_id)
    current_balance = p_data['balance']
    my_p_obj = table.get_player(player.user_id)
    session_diff = 0
    if my_p_obj:
        session_diff = current_balance - my_p_obj.start_balance
    
    diff_str = f"+{session_diff}" if session_diff > 0 else f"{session_diff}"
    
    shoe_bar = table.deck.get_visual_bar()
    shuffle_alert = " 🔄 SHUFFLE" if table.shuffle_alert else ""
    
    info_section = (
        f"───────────────\n"
        f"👝 Баланс: *{current_balance}* ({diff_str})\n"
        f"🃏 Шу: {shoe_bar}{shuffle_alert}"
    )

    chat_section = "\n───────────────\n"
    if table.chat_history:
        chat_section += "\n".join([f"▫️ {msg}" for msg in table.chat_history]) + "\n"
    chat_section += "✎ _Напишите сообщение в этот чат_"

    final_text = (
        f"🎰 *TABLE #{table.id}*\n"
        f"───────────────\n"
        f"{dealer_section}"
        f"───────────────\n"
        f"{players_section}"
        f"{info_section}"
        f"{chat_section}"
    )
    
    return final_text

def get_game_kb(table: GameTable, player: TablePlayer):
    if table.state == "finished":
        if not table.is_public:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Играть еще", callback_data=f"replay_{table.id}")],
                [InlineKeyboardButton(text="💰 Изм. ставку", callback_data="play_solo")],
                [InlineKeyboardButton(text="🚪 Меню", callback_data="menu")]
            ])
        else:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Продолжить", callback_data=f"rematch_{table.id}")],
                [InlineKeyboardButton(text="🚪 Выйти", callback_data=f"leave_lobby_{table.id}")]
            ])

    current_p = table.players[table.current_player_index]
    if current_p != player:
        return None 

    kb = [
        [InlineKeyboardButton(text="🖐 HIT", callback_data=f"hit_{table.id}"),
         InlineKeyboardButton(text="✋ STAND", callback_data=f"stand_{table.id}")]
    ]
    if len(player.hand) == 2:
        kb.insert(0, [InlineKeyboardButton(text="2️⃣ x2", callback_data=f"double_{table.id}")])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)


async def update_table_messages(table_id):
    table = tables.get(table_id)
    if not table: return

    if not table.players:
        del tables[table_id]
        return

    if table.state == "waiting":
        txt = render_lobby(table)
        for p in table.players:
            if p.message_id:
                kb = get_lobby_kb(table, p.user_id)
                try:
                    await bot.edit_message_text(txt, chat_id=p.user_id, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
                except TelegramBadRequest: pass
        return

    for p in table.players:
        if p.message_id:
            txt = await render_table_for_player(table, p, bot)
            kb = get_game_kb(table, p)
            try:
                await bot.edit_message_text(txt, chat_id=p.user_id, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
            except TelegramBadRequest: pass

async def finalize_game_db(table: GameTable):
    d_val = table._hand_value(table.dealer_hand)
    
    for p in table.players:
        data = await get_player_data(p.user_id) 
        p_username = data.get('username', 'Unknown')
        stats = data['stats']
        bal = data['balance']
        
        result_type = "loss"
        win_amount = 0
        
        if p.status == "bust":
            win_amount = -p.bet
            stats['losses'] += 1
            result_type = "loss"
        elif p.status == "blackjack":
             win_amount = int(p.bet * 1.5)
             stats['wins'] += 1
             stats['blackjacks'] += 1
             result_type = "blackjack"
        elif d_val > 21 or p.value > d_val:
            win_amount = p.bet
            stats['wins'] += 1
            result_type = "win"
        elif p.value < d_val:
            win_amount = -p.bet
            stats['losses'] += 1
            result_type = "loss"
        else:
            win_amount = 0
            stats['pushes'] += 1
            result_type = "push"

        new_bal = bal + win_amount
        stats['games'] += 1
        stats['max_balance'] = max(stats['max_balance'], new_bal)
        if win_amount > 0: stats['max_win'] = max(stats['max_win'], win_amount)
            
        await update_player_stats(p.user_id, new_bal, stats)
        # ЛОГИРУЕМ ИГРУ С ЮЗЕРНЕЙМОМ
        await log_game(table.id, p.user_id, p_username, p.bet, result_type, win_amount, p.hand, table.dealer_hand)

# ====== ХЕНДЛЕРЫ ======

class BetState(StatesGroup):
    waiting = State()
    
class MultiCustomBet(StatesGroup):
    waiting = State()

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    data = await get_player_data(message.from_user.id, message.from_user.username)
    s = data['stats']
    name = f"@{data['username']}" if data['username'] else message.from_user.first_name

    text = (
        f"🎩 *Blackjack Revolution*\n"
        f"_Искусство побеждать. Стратегия, удача и холодный расчет._\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👤 *Профиль:* {name}\n"
        f"💼 *Счет:* {data['balance']} 🪙\n"
        f"🏆 *Побед:* {s['wins']}\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🎲 _Столы открыты. Делайте ваши ставки._"
    )
    
    await message.answer(text, parse_mode="Markdown", reply_markup=main_menu_kb())

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Одиночная игра", callback_data="play_solo")],
        [InlineKeyboardButton(text="👥 Онлайн столы", callback_data="play_multi")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])

@dp.callback_query(lambda c: c.data == "menu")
async def cb_menu(call: CallbackQuery):
    data = await get_player_data(call.from_user.id, call.from_user.username)
    s = data['stats']
    name = f"@{data['username']}" if data['username'] else call.from_user.first_name
    
    text = (
        f"🎩 *Blackjack Revolution*\n"
        f"_Искусство побеждать. Стратегия, удача и холодный расчет._\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👤 *Профиль:* {name}\n"
        f"💼 *Счет:* {data['balance']} 🪙\n"
        f"🏆 *Побед:* {s['wins']}\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"🎲 _Столы открыты. Делайте ваши ставки._"
    )
    await call.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())

# -- СОЛО --
@dp.callback_query(lambda c: c.data == "play_solo")
async def cb_play_solo(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    kb = [[InlineKeyboardButton(text=f"💰 {b}", callback_data=f"start_solo_{b}")] for b in BET_OPTIONS]
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data="custom_bet")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="menu")])
    await call.message.edit_text(f"🪙 Баланс: {data['balance']}\nВыберите ставку:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("start_solo_"))
async def cb_start_solo(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet: return await call.answer("Мало денег!", show_alert=True)
    
    leave_all_tables(call.from_user.id)

    tid = str(uuid.uuid4())[:8]
    table = GameTable(tid, is_public=False, owner_id=call.from_user.id)
    tables[tid] = table
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet, current_balance=data['balance'])
    
    table.start_game()
    txt = await render_table_for_player(table, p, bot)
    kb = get_game_kb(table, p)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id
    if table.state == "finished":
        await finalize_game_db(table)
        await update_table_messages(tid)

# -- Кастомная ставка (СОЛО) --
@dp.callback_query(lambda c: c.data == "custom_bet")
async def cb_custom_input(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("✍️ Введите ставку:")
    await state.set_state(BetState.waiting)

@dp.message(BetState.waiting)
async def process_custom_bet(message: types.Message, state: FSMContext):
    try:
        bet = int(message.text)
        if bet <= 0: raise ValueError
        data = await get_player_data(message.from_user.id)
        if data['balance'] < bet:
            await message.answer("Недостаточно средств!")
            return
        
        leave_all_tables(message.from_user.id)
        
        tid = str(uuid.uuid4())[:8]
        table = GameTable(tid, is_public=False, owner_id=message.from_user.id)
        tables[tid] = table
        p = table.add_player(message.from_user.id, message.from_user.first_name, bet, current_balance=data['balance'])
        
        table.start_game()
        txt = await render_table_for_player(table, p, bot)
        kb = get_game_kb(table, p)
        msg = await message.answer(txt, reply_markup=kb, parse_mode="Markdown")
        p.message_id = msg.message_id
        if table.state == "finished":
            await finalize_game_db(table)
            await update_table_messages(tid)
        await state.clear()
    except:
        await message.answer("Ошибка. Введите целое число > 0")

# ЛОГИКА REPLAY СОЛО
@dp.callback_query(lambda c: c.data.startswith("replay_"))
async def cb_replay(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    
    if not table:
         await call.answer("Сессия истекла", show_alert=True)
         return await cb_play_solo(call)
    
    leave_all_tables(call.from_user.id, exclude_tid=tid)
    
    p = table.players[0]
    
    data = await get_player_data(p.user_id)
    if data['balance'] < p.original_bet: 
        await call.answer("Недостаточно средств!", show_alert=True)
        return
    
    table.start_game()
    await update_table_messages(tid)
    
    if table.state == "finished":
        await finalize_game_db(table)
        await update_table_messages(tid)

# -- МУЛЬТИПЛЕЕР: СПИСОК СТОЛОВ --
@dp.callback_query(lambda c: c.data == "play_multi" or c.data == "refresh_multi")
async def cb_play_multi(call: CallbackQuery):
    waiting_tables = [t for t in tables.values() if t.is_public and t.state == "waiting"]
    
    kb = []
    for t in waiting_tables[:5]: 
        owner_name = t.players[0].name if t.players else "Неизвестно"
        players_cnt = len(t.players)
        btn_text = f"👤 {owner_name} | 👥 {players_cnt}/{MAX_PLAYERS}"
        kb.append([InlineKeyboardButton(text=btn_text, callback_data=f"prejoin_{t.id}")])
    
    if not waiting_tables:
         kb.append([InlineKeyboardButton(text="📭 Нет активных столов", callback_data="noop")])

    kb.append([InlineKeyboardButton(text="➕ Создать стол", callback_data="create_table_setup")])
    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_multi")]) 
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="menu")])
    
    text = "👥 *Онлайн Лобби*\nНажмите на стол, чтобы присоединиться:"
    
    if call.data == "refresh_multi":
         try: await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
         except TelegramBadRequest: await call.answer("Список актуален")
    else:
         await call.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer("В данный момент нет открытых столов. Создайте свой!")

# -- 1. Создание стола --
@dp.callback_query(lambda c: c.data == "create_table_setup")
async def cb_create_setup(call: CallbackQuery):
    kb = [[InlineKeyboardButton(text=f"💰 {b}", callback_data=f"new_multi_{b}")] for b in BET_OPTIONS]
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data="multi_custom_create")])
    kb.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="play_multi")])
    await call.message.edit_text("С какой ставкой вы хотите создать стол?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("new_multi_"))
async def cb_new_multi_created(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    await create_multi_table(call, bet)

@dp.callback_query(lambda c: c.data == "multi_custom_create")
async def cb_multi_custom_create_input(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("✍️ Введите ставку для стола:")
    await state.set_state(MultiCustomBet.waiting)
    await state.update_data(mode="create")

async def create_multi_table(call: CallbackQuery, bet: int):
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet: return await call.answer("Не хватает денег!", show_alert=True)
    
    leave_all_tables(call.from_user.id)
    
    tid = str(uuid.uuid4())[:5]
    table = GameTable(tid, is_public=True, owner_id=call.from_user.id)
    tables[tid] = table
    
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet, current_balance=data['balance'])
    
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id

# -- 2. Присоединение к столу --
@dp.callback_query(lambda c: c.data.startswith("prejoin_"))
async def cb_prejoin(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    if not table or table.state != "waiting":
        return await call.answer("Стол недоступен", show_alert=True)
    if len(table.players) >= MAX_PLAYERS:
        return await call.answer("Стол полон", show_alert=True)
    if table.get_player(call.from_user.id):
        return await call.answer("Вы уже за этим столом")

    kb = [[InlineKeyboardButton(text=f"💰 {b}", callback_data=f"joinbet_{tid}_{b}")] for b in BET_OPTIONS]
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data=f"multi_custom_join_{tid}")])
    kb.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="play_multi")])
    await call.message.edit_text(f"Вы входите за стол #{tid}.\nВаша ставка?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("multi_custom_join_"))
async def cb_multi_custom_join_input(call: CallbackQuery, state: FSMContext):
    tid = call.data.split("_")[3]
    await call.message.edit_text(f"✍️ Введите ставку для входа (Стол #{tid}):")
    await state.set_state(MultiCustomBet.waiting)
    await state.update_data(mode="join", tid=tid)

@dp.message(MultiCustomBet.waiting)
async def process_multi_custom_bet(message: types.Message, state: FSMContext):
    try:
        bet = int(message.text)
        if bet <= 0: raise ValueError
        
        user_data = await state.get_data()
        mode = user_data.get("mode")
        
        p_data = await get_player_data(message.from_user.id)
        if p_data['balance'] < bet:
            await message.answer("Недостаточно средств!")
            return
            
        if mode == "create":
            leave_all_tables(message.from_user.id) 
            
            tid = str(uuid.uuid4())[:5]
            table = GameTable(tid, is_public=True, owner_id=message.from_user.id)
            tables[tid] = table
            p = table.add_player(message.from_user.id, message.from_user.first_name, bet, current_balance=p_data['balance'])
            
            txt = render_lobby(table)
            kb = get_lobby_kb(table, p.user_id)
            msg = await message.answer(txt, reply_markup=kb, parse_mode="Markdown")
            p.message_id = msg.message_id
            
        elif mode == "join":
            tid = user_data.get("tid")
            await join_multi_table(message, tid, bet)
            
        elif mode == "rebet":
            tid = user_data.get("tid")
            await rebet_multi_table(message, tid, bet)
            
        await state.clear()
            
    except ValueError:
        await message.answer("Введите целое число > 0")

async def join_multi_table(msg_obj, tid, bet):
    table = tables.get(tid)
    if not table or table.state != "waiting":
         return await msg_obj.answer("Стол исчез или игра началась.")
    
    if table.get_player(msg_obj.from_user.id):
        return await msg_obj.answer("Вы уже здесь!")

    leave_all_tables(msg_obj.from_user.id)
    
    data = await get_player_data(msg_obj.from_user.id)
    p = table.add_player(msg_obj.from_user.id, msg_obj.from_user.first_name, bet, current_balance=data['balance'])
    
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    sent_msg = await msg_obj.answer(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = sent_msg.message_id
    
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("joinbet_"))
async def cb_join_confirm(call: CallbackQuery):
    parts = call.data.split("_") 
    tid = parts[1]
    bet = int(parts[2])
    
    table = tables.get(tid)
    if not table or table.state != "waiting":
         return await call.message.edit_text("Стол исчез или игра началась.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Ок", callback_data="play_multi")]]))
    
    if table.get_player(call.from_user.id):
        return await call.answer("Вы уже здесь!")
    
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet:
        return await call.answer("Не хватает денег!", show_alert=True)

    leave_all_tables(call.from_user.id)

    p = table.add_player(call.from_user.id, call.from_user.first_name, bet, current_balance=data['balance'])
    
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id
    
    await update_table_messages(tid)

# -- ГОТОВНОСТЬ (READY) --
@dp.callback_query(lambda c: c.data.startswith("ready_"))
async def cb_ready(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    if not table: return await call.answer("Стол не найден")
    
    p = table.get_player(call.from_user.id)
    if not p: return
    
    p.is_ready = True
    await call.answer("Вы готовы!")
    
    if table.check_all_ready():
        table.start_game()
        await update_table_messages(tid)
        if table.state == "finished":
            await finalize_game_db(table)
            await update_table_messages(tid)
    else:
        await update_table_messages(tid)

# -- РЕВАНШ / СМЕНА СТАВКИ --
@dp.callback_query(lambda c: c.data.startswith("rematch_") or c.data.startswith("chbet_lobby_"))
async def cb_rematch_or_change(call: CallbackQuery):
    parts = call.data.split("_")
    tid = parts[-1] 
    
    table = tables.get(tid)
    if not table: return await cb_play_multi(call)
    
    p = table.get_player(call.from_user.id)
    if not p: return await cb_play_multi(call)
    
    kb = []
    kb.append([InlineKeyboardButton(text=f"Оставить: {p.original_bet}", callback_data=f"m_rebet_{tid}_{p.original_bet}")])
    row = []
    for b in BET_OPTIONS:
         row.append(InlineKeyboardButton(text=f"{b}", callback_data=f"m_rebet_{tid}_{b}"))
    kb.append(row)
    
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data=f"multi_custom_rebet_{tid}")])
    kb.append([InlineKeyboardButton(text="🔙 Отмена (Выйти)", callback_data=f"leave_lobby_{tid}")])
    
    await call.message.edit_text(f"💰 Ставка на следующий раунд?\n(Текущая: {p.original_bet})", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("multi_custom_rebet_"))
async def cb_multi_custom_rebet_input(call: CallbackQuery, state: FSMContext):
    tid = call.data.split("_")[3]
    await call.message.edit_text(f"✍️ Введите новую ставку (Стол #{tid}):")
    await state.set_state(MultiCustomBet.waiting)
    await state.update_data(mode="rebet", tid=tid)

async def rebet_multi_table(message, tid, bet):
    table = tables.get(tid)
    if not table: 
        await message.answer("Стол не найден")
        return
    
    p = table.get_player(message.from_user.id)
    if not p: return 
    
    p.original_bet = bet
    p.bet = bet
    p.is_ready = False 
    
    if table.state == "finished":
        table.reset_round()
        
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    sent_msg = await message.answer(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = sent_msg.message_id
    
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("m_rebet_"))
async def cb_multi_rebet(call: CallbackQuery):
    parts = call.data.split("_")
    tid = parts[2]
    bet = int(parts[3])
    
    table = tables.get(tid)
    if not table: return await cb_play_multi(call)
    
    p = table.get_player(call.from_user.id)
    if not p: return 
    
    data = await get_player_data(p.user_id)
    if data['balance'] < bet:
        return await call.answer("Не хватает денег!", show_alert=True)
    
    # Обновляем ставку
    p.original_bet = bet
    p.bet = bet
    p.is_ready = False 
    
    if table.state == "finished":
        table.reset_round()
        
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    try:
        await bot.edit_message_text(txt, chat_id=p.user_id, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
    except: pass
    
    await update_table_messages(tid)


@dp.callback_query(lambda c: c.data.startswith("leave_lobby_"))
async def cb_leave_lobby(call: CallbackQuery):
    tid = call.data.split("_")[2]
    table = tables.get(tid)
    if table:
        table.remove_player(call.from_user.id)
        await update_table_messages(tid)
    await cb_play_multi(call) 

@dp.callback_query(lambda c: c.data.startswith("close_lobby_"))
async def cb_close_lobby(call: CallbackQuery):
    tid = call.data.split("_")[2]
    table = tables.get(tid)
    if table:
        for p in table.players:
            if p.user_id != table.owner_id: 
                 try: await bot.send_message(p.user_id, "Стол был закрыт владельцем.")
                 except: pass
        del tables[tid]
    await cb_play_multi(call)

# -- GAME ACTIONS --
@dp.callback_query(lambda c: c.data.startswith("hit_"))
async def cb_hit(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    if not table: return await call.answer("Ошибка")
    player = table.get_player(call.from_user.id)
    if not player or table.players[table.current_player_index] != player: return await call.answer("Не твой ход!")
    
    c, s = table.deck.get_card()
    if s: table.shuffle_alert = True
    player.hand.append(c)
    player.last_action = "hit" 

    if player.value > 21:
        player.status = "bust"
        await call.answer("Перебор!", show_alert=False)
        table.process_turns()
    elif player.value == 21:
        player.status = "stand"
        await call.answer("21! Стоп.", show_alert=False)
        table.process_turns()
        
    if table.state == "finished": await finalize_game_db(table)
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("stand_"))
async def cb_stand(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    if not table: return
    player = table.get_player(call.from_user.id)
    if not player or table.players[table.current_player_index] != player: return await call.answer("Не твой ход!")
        
    player.status = "stand"
    player.last_action = "stand" 
    await call.answer("Стоп.")
    table.process_turns()
    if table.state == "finished": await finalize_game_db(table)
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("double_"))
async def cb_double(call: CallbackQuery):
    tid = call.data.split("_")[1]
    table = tables.get(tid)
    if not table: return
    player = table.get_player(call.from_user.id)
    if not player or table.players[table.current_player_index] != player: return await call.answer("Не твой ход!")
    
    data = await get_player_data(player.user_id)
    if data['balance'] < player.bet * 2: return await call.answer("Не хватает фишек!", show_alert=True)
    
    player.bet *= 2
    c, s = table.deck.get_card()
    player.hand.append(c)
    player.last_action = "double" 
    
    if player.value > 21: player.status = "bust"
    else: player.status = "stand"
    
    await call.answer("Удвоение!")
    table.process_turns()
    if table.state == "finished": await finalize_game_db(table)
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data == "stats")
async def cb_stats(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    s = data['stats']
    
    total_games = s['games']
    win_rate = round((s['wins'] / total_games * 100), 1) if total_games > 0 else 0
    
    net_profit = data['balance'] - 1000
    net_str = f"+{net_profit}" if net_profit > 0 else f"{net_profit}"

    stats_text = (
        f"📊 *Личная статистика*\n\n"
        f"🎮 Игры: *{s['games']}*\n"
        f"🏆 Победы: *{s['wins']}*\n"
        f"💀 Поражения: *{s['losses']}*\n"
        f"🤝 Ничьи: *{s['pushes']}*\n"
        f"🃏 Blackjack: *{s['blackjacks']}*\n"
        f"📈 Win Rate: *{win_rate}%*\n\n"
        f"🪙 Баланс: *{data['balance']}*\n"
        f"💵 Профит: *{net_str}*\n"
        f"🏦 Макс. баланс: *{s['max_balance']}*\n"
        f"🤑 Макс. выигрыш: *{s['max_win']}*\n\n"
        f"🆔 ID: `{call.from_user.id}`"
    )
    
    await call.message.edit_text(
        stats_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Меню", callback_data="menu")]])
    )

# ====== CHAT HANDLER ======
@dp.message(F.text)
async def process_table_chat(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        return

    try:
        await message.delete()
    except:
        pass 

    user_id = message.from_user.id
    target_table = None
    
    for table in tables.values():
        if table.get_player(user_id):
            target_table = table
            break
            
    if target_table:
        target_table.add_chat_message(message.from_user.first_name, message.text)
        await update_table_messages(target_table.id)
        # ЛОГИРУЕМ ЧАТ
        await log_chat(target_table.id, user_id, message.from_user.username, message.text)




def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🃏 Играть Solo", callback_data="play_solo"),
         InlineKeyboardButton(text="👥 Играть Multi", callback_data="play_multi")],
        [InlineKeyboardButton(text="🎁 Получить фишки", callback_data="free_chips")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])



@dp.callback_query(lambda c: c.data == "free_chips")
async def cb_free_chips(call: CallbackQuery):
    user_id = call.from_user.id
    # 9:00 MSK = 6:00 UTC. We want the "day" to switch at 6:00 UTC.
    # Current UTC time
    now_utc = datetime.now(timezone.utc)
    # To determine the "bonus day", we subtract 6 hours.
    # If it's 5:59 UTC (8:59 MSK), subtracting 6h puts us in the previous day.
    # If it's 6:01 UTC (9:01 MSK), subtracting 6h keeps us in the current day.
    current_bonus_day = (now_utc - timedelta(hours=6)).date()

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT last_bonus_date FROM users WHERE userid = $1", user_id)

        if not row or row['last_bonus_date'] != current_bonus_day:
            bonus = 1000
            await conn.execute("""
                UPDATE users 
                SET balance = balance + $1, last_bonus_date = $2 
                WHERE userid = $3
            """, bonus, current_bonus_day, user_id)

            await call.answer(f"🎁 Вы получили {bonus} фишек!", show_alert=True)

            # Update menu with new balance
            data = await get_player_data(user_id)
            name = f"@{data['username']}" if data['username'] else call.from_user.first_name
            text = (f"🎰 **Blackjack Revolution**\n\n"
                    f"👤 {name}\n"
                    f"💰 Баланс: **{data['balance']}**\n"
                    f"🎁 Ежедневный бонус получен!")
            try:
                await call.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())
            except TelegramBadRequest:
                pass
        else:
            # Calculate time until next 9:00 MSK (6:00 UTC)
            # The next bonus is available on (current_bonus_day + 1 day) at 6:00 UTC
            next_bonus_time = datetime.combine(current_bonus_day + timedelta(days=1), dt_time(6, 0), tzinfo=timezone.utc)
            delta = next_bonus_time - now_utc

            total_seconds = int(delta.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60

            await call.answer(f"⏳ Вы сегодня уже получили фишки\nДо следующего получения: {hours}ч {minutes}мин", show_alert=True)

            @dp.message(Command("fixdb"))
async def cmd_fixdb(message: types.Message):
    async with pool.acquire() as conn:
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_date DATE")
            await message.answer("✅ База данных обновлена: колонка last_bonus_date добавлена.")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")

async def main():
    await init_db()
    print("Bot started")
    asyncio.create_task(check_timeouts_loop()) # Запуск фоновой проверки таймаутов
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
