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

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN or not DATABASE_URL:
    raise ValueError("No TOKEN or DATABASE_URL provided")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- МЕНЮ (С КНОПКОЙ БОНУСА) ---
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🃏 Играть Solo", callback_data="play_solo"),
         InlineKeyboardButton(text="👥 Играть Multi", callback_data="play_multi")],
        [InlineKeyboardButton(text="🎁 Получить фишки", callback_data="free_chips")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])

# --- ИГРОВЫЕ КОНСТАНТЫ ---
RANKS = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
SUITS = ["♥️", "♦️", "♣️", "♠️"]
DECKS_COUNT = 5
TOTAL_CARDS = 52 * DECKS_COUNT
RESHUFFLE_THRESHOLD = 60
BET_OPTIONS = [50, 100, 250]
MAX_PLAYERS = 3
TURN_TIMEOUT = 30

pool = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                userid BIGINT PRIMARY KEY,
                username TEXT,
                balance INTEGER DEFAULT 1000,
                games INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                pushes INTEGER DEFAULT 0,
                blackjacks INTEGER DEFAULT 0,
                maxbalance INTEGER DEFAULT 1000,
                maxwin INTEGER DEFAULT 0
            )
        """)
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT")
        except: pass
        
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_bonus_date DATE")
        except: pass
            
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS gamelogs (
                id SERIAL PRIMARY KEY,
                tableid TEXT,
                userid BIGINT,
                username TEXT,
                bet INTEGER,
                result TEXT,
                winamount INTEGER,
                playerhand TEXT,
                dealerhand TEXT,
                createdat TIMESTAMP DEFAULT NOW()
            )
        """)
        try:
            await conn.execute("ALTER TABLE gamelogs ADD COLUMN IF NOT EXISTS username TEXT")
        except: pass

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chatlogs (
                id SERIAL PRIMARY KEY,
                tableid TEXT,
                userid BIGINT,
                username TEXT,
                message TEXT,
                createdat TIMESTAMP DEFAULT NOW()
            )
        """)
    print("Database initialized")

async def get_player_data(userid, username=None):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE userid = $1", userid)
        if not row:
            await conn.execute("""
                INSERT INTO users (userid, username, balance, maxbalance, maxwin) 
                VALUES ($1, $2, 1000, 1000, 0) 
                ON CONFLICT (userid) DO NOTHING
            """, userid, username)
            return {"balance": 1000, "username": username, "stats": {"games":0, "wins":0, "losses":0, "pushes":0, "blackjacks":0, "maxbalance":1000, "maxwin":0}}
        
        if username and row['username'] != username:
            await conn.execute("UPDATE users SET username = $2 WHERE userid = $1", userid, username)
            
        return {
            "balance": row['balance'],
            "username": row['username'],
            "stats": {
                "games": row['games'], "wins": row['wins'], "losses": row['losses'], 
                "pushes": row['pushes'], "blackjacks": row['blackjacks'],
                "maxbalance": row['maxbalance'], "maxwin": row.get('maxwin', 0) or 0
            }
        }

async def update_player_stats(userid, balance, stats):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET balance=$2, games=$3, wins=$4, losses=$5, pushes=$6, blackjacks=$7, maxbalance=$8, maxwin=$9 
            WHERE userid=$1
        """, userid, balance, stats['games'], stats['wins'], stats['losses'], stats['pushes'], stats['blackjacks'], stats['maxbalance'], stats['maxwin'])

async def log_game(tableid, userid, username, bet, result, winamount, phand, dhand):
    async with pool.acquire() as conn:
        str_phand = " ".join([f"{c[0]}{c[1]}" for c in phand])
        str_dhand = " ".join([f"{c[0]}{c[1]}" for c in dhand])
        await conn.execute("""
            INSERT INTO gamelogs (tableid, userid, username, bet, result, winamount, playerhand, dealerhand) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, tableid, userid, username, bet, result, winamount, str_phand, str_dhand)

async def log_chat(tableid, userid, username, message):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chatlogs (tableid, userid, username, message) 
            VALUES ($1, $2, $3, $4)
        """, tableid, userid, username, message)

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
        bar = "🟩" * blocks + "⬜" * (8 - blocks)
        return f"{bar} {int(percent*100)}%"

class TablePlayer:
    def __init__(self, userid, name, bet, start_balance):
        self.userid = userid
        self.name = name
        self.bet = bet
        self.original_bet = bet
        self.hand = []
        self.status = "waiting"
        self.is_ready = False
        self.message_id = None
        self.start_balance = start_balance
        self.last_action = None

    @property
    def value(self):
        val = sum(10 if c[0] in ["J","Q","K"] else 11 if c[0]=="A" else int(c[0]) for c in self.hand)
        aces = sum(1 for c in self.hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

    def render_hand(self):
        if not self.hand: return ""
        return " ".join([f"{r}{s}" for r, s in self.hand])

class GameTable:
    def __init__(self, table_id, is_public=False, owner_id=None):
        self.id = table_id
        self.is_public = is_public
        self.owner_id = owner_id
        self.players = []
        self.dealer_hand = []
        self.deck = CardSystem()
        self.state = "waiting"
        self.current_player_index = 0
        self.shuffle_alert = False
        self.last_action_time = time.time()
        self.chat_history = []

    def add_player(self, userid, name, bet, current_balance):
        player = TablePlayer(userid, name, bet, current_balance)
        self.players.append(player)
        self.update_activity()
        return player

    def remove_player(self, userid):
        self.players = [p for p in self.players if p.userid != userid]
        if userid == self.owner_id:
            self.owner_id = self.players[0].userid if self.players else None
        self.update_activity()

    def get_player(self, userid):
        for p in self.players:
            if p.userid == userid: return p
        return None

    def add_chat_message(self, name, text):
        clean_text = text[:30]
        self.chat_history.append(f"{name}: {clean_text}")
        if len(self.chat_history) > 5: self.chat_history.pop(0)

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
            if p.value == 21: p.status = "blackjack"
        
        self.state = "player_turn"
        self.current_player_index = 0
        self.process_turns()

    def process_turns(self):
        self.update_activity()
        while self.current_player_index < len(self.players):
            p = self.players[self.current_player_index]
            if p.status == "playing": return
            self.current_player_index += 1
        self.state = "dealer_turn"
        self.play_dealer()

    def play_dealer(self):
        val = self.hand_value(self.dealer_hand)
        while val < 17:
            c, s = self.deck.get_card()
            if s: self.shuffle_alert = True
            self.dealer_hand.append(c)
            val = self.hand_value(self.dealer_hand)
        self.state = "finished"

    def hand_value(self, hand):
        val = sum(10 if c[0] in ["J","Q","K"] else 11 if c[0]=="A" else int(c[0]) for c in hand)
        aces = sum(1 for c in hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

tables = {}

def leave_all_tables(userid, exclude_tid=None):
    for tid in list(tables.keys()):
        if tid == exclude_tid: continue
        table = tables.get(tid)
        if table and table.get_player(userid):
            table.remove_player(userid)
            if not table.players: del tables[tid]

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
                        try:
                            await bot.send_message(current_p.userid, "⏰ Время вышло! Авто-Stand.")
                        except: pass
                    except IndexError: pass

def render_lobby_table(table: GameTable):
    txt = f"🎲 **BLACKJACK TABLE {table.id}**\n"
    txt += f"👤 Owner: {table.players[0].name}\n\n"
    for i, p in enumerate(table.players, 1):
        role = "👑" if p.userid == table.owner_id else "👤"
        status = "✅" if p.is_ready else "⏳"
        txt += f"{i}. {status} {role} {p.name} — 💰 {p.bet}\n"
    txt += f"\nИгроков: {len(table.players)}/{MAX_PLAYERS}\n"
    if table.chat_history:
        txt += "\n💬 **LIVE CHAT:**\n" + "\n".join([f"`{msg}`" for msg in table.chat_history])
    else:
        txt += "\n💬 Чат пуст. Пиши сюда!"
    return txt

def get_lobby_kb(table: GameTable, userid):
    kb = []
    p = table.get_player(userid)
    if not p.is_ready:
        kb.append([InlineKeyboardButton(text="✅ ГОТОВ", callback_data=f"ready_{table.id}")])
        kb.append([InlineKeyboardButton(text="💵 Изм. ставку", callback_data=f"chbetlobby_{table.id}")])
    kb.append([InlineKeyboardButton(text="🚪 Выйти", callback_data=f"leavelobby_{table.id}")])
    if p.userid == table.owner_id:
        kb.append([InlineKeyboardButton(text="❌ Закрыть стол", callback_data=f"closelobby_{table.id}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def render_table_for_player(table: GameTable, player: TablePlayer, bot: Bot):
    if table.state == "finished":
        d_val = table.hand_value(table.dealer_hand)
        d_cards = " ".join([f"{c[0]}{c[1]}" for c in table.dealer_hand])
        dealer_section = f"👤 **DEALER**\n`{d_cards}` | **{d_val}**"
    else:
        visible = [table.dealer_hand[0]]
        vis_val = table.hand_value(visible)
        d_cards = f"{visible[0][0]}{visible[0][1]} 🎴"
        dealer_section = f"👤 **DEALER**\n`{d_cards}` | **{vis_val}**"

    players_section = ""
    for p in table.players:
        status_marker = "🔸"
        status_text = ""
        action_trail = ""
        
        if p.last_action == "hit": action_trail = "👊 HIT"
        elif p.last_action == "stand": action_trail = "🛑 STAND"
        elif p.last_action == "double": action_trail = "💰 x2 DOUBLE"

        if table.state == "player_turn":
            if table.players[table.current_player_index] == p:
                status_marker = "▶️"
                action_trail = "🤔 thinking..."
            elif table.players.index(p) < table.current_player_index:
                status_marker = "zzz"
            else:
                status_marker = "⏳"
        elif table.state == "finished":
            d_val = table.hand_value(table.dealer_hand)
            if p.status == "bust":
                status_marker = "💀"
                status_text = "BUST"
            elif p.status == "blackjack":
                status_marker = "💎"
                status_text = f"BLACKJACK! +{int(p.bet * 1.5)}"
            elif d_val > 21 or (p.value <= 21 and p.value > d_val):
                status_marker = "🏆"
                status_text = f"WIN +{p.bet}"
            elif p.value == d_val:
                status_marker = "🤝"
                status_text = "PUSH"
            else:
                status_marker = "💸"
                status_text = "LOSE"

        is_me = " (YOU)" if p.userid == player.userid else ""
        name_line = f"{status_marker} *{p.name}{is_me}* {action_trail}\n"
        cards_line = f"   `{p.render_hand()}` | **{p.value}**\n"
        full_status_line = f"   _{status_text}_\n" if status_text else ""
        players_section += f"{name_line}{cards_line}{full_status_line}\n"

    p_data = await get_player_data(player.userid)
    current_balance = p_data['balance']
    my_p_obj = table.get_player(player.userid)
    session_diff = 0
    if my_p_obj:
        session_diff = current_balance - my_p_obj.start_balance
    
    diff_str = f"+{session_diff}" if session_diff > 0 else f"{session_diff}"
    shoe_bar = table.deck.get_visual_bar()
    shuffle_alert = "\n🔀 **SHUFFLE SOON**" if table.shuffle_alert else ""
    
    info_section = f"💰 **{current_balance}** ({diff_str})\n🃏 Shoe: {shoe_bar}{shuffle_alert}"
    chat_section = ""
    if table.chat_history:
        chat_section = "\n\n💬 **Chat:**\n" + "\n".join([f"`{msg}`" for msg in table.chat_history])
        
    final_text = f"🎲 **TABLE {table.id}**\n\n{dealer_section}\n\n{players_section}\n{info_section}{chat_section}"
    return final_text

def get_game_kb(table: GameTable, player: TablePlayer):
    if table.state == "finished":
        if not table.is_public:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Replay", callback_data=f"replay_{table.id}"),
                 InlineKeyboardButton(text="🔙 Menu", callback_data="menu")]
            ])
        else:
             return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Ready", callback_data=f"rematch_{table.id}"),
                 InlineKeyboardButton(text="🚪 Leave", callback_data=f"leavelobby_{table.id}")]
            ])

    current_p = table.players[table.current_player_index]
    if current_p != player: return None
    
    kb = [
        [InlineKeyboardButton(text="👊 HIT", callback_data=f"hit_{table.id}"),
         InlineKeyboardButton(text="🛑 STAND", callback_data=f"stand_{table.id}")]
    ]
    if len(player.hand) == 2:
        kb[0].insert(1, InlineKeyboardButton(text="💰 x2", callback_data=f"double_{table.id}"))
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def update_table_messages(table_id):
    table = tables.get(table_id)
    if not table: return
    if not table.players:
        del tables[table_id]
        return

    if table.state == "waiting":
        txt = render_lobby_table(table)
        for p in table.players:
            if p.message_id:
                kb = get_lobby_kb(table, p.userid)
                try: await bot.edit_message_text(txt, chat_id=p.userid, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
                except TelegramBadRequest: pass
        return

    for p in table.players:
        if p.message_id:
            txt = await render_table_for_player(table, p, bot)
            kb = get_game_kb(table, p)
            try: await bot.edit_message_text(txt, chat_id=p.userid, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
            except TelegramBadRequest: pass

async def finalize_game_db(table: GameTable):
    d_val = table.hand_value(table.dealer_hand)
    for p in table.players:
        data = await get_player_data(p.userid)
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
        elif d_val > 21 or (p.value <= 21 and p.value > d_val):
            win_amount = p.bet
            stats['wins'] += 1
            result_type = "win"
        elif p.value == d_val:
            win_amount = 0
            stats['pushes'] += 1
            result_type = "push"
        else:
            win_amount = -p.bet
            stats['losses'] += 1
            result_type = "loss"

        new_bal = bal + win_amount
        stats['games'] += 1
        stats['maxbalance'] = max(stats['maxbalance'], new_bal)
        if win_amount > 0: stats['maxwin'] = max(stats['maxwin'], win_amount)
        
        await update_player_stats(p.userid, new_bal, stats)
        await log_game(table.id, p.userid, p.name, p.bet, result_type, win_amount, p.hand, table.dealer_hand)

# --- FSM STATES ---
class BetState(StatesGroup): waiting = State()
class MultiCustomBet(StatesGroup): waiting = State()

# --- HANDLERS ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    data = await get_player_data(message.from_user.id, message.from_user.username)
    s = data['stats']
    name = f"@{data['username']}" if data['username'] else message.from_user.first_name
    text = (f"🎰 **Blackjack Revolution**\n\n"
            f"👤 {name}\n"
            f"💰 Баланс: **{data['balance']}**\n"
            f"🏆 Побед: {s['wins']}")
    await message.answer(text, parse_mode="Markdown", reply_markup=main_menu_kb())

@dp.callback_query(lambda c: c.data == "menu")
async def cb_menu(call: CallbackQuery):
    data = await get_player_data(call.from_user.id, call.from_user.username)
    s = data['stats']
    name = f"@{data['username']}" if data['username'] else call.from_user.first_name
    text = (f"🎰 **Blackjack Revolution**\n\n"
            f"👤 {name}\n"
            f"💰 Баланс: **{data['balance']}**\n"
            f"🏆 Побед: {s['wins']}")
    await call.message.edit_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())

@dp.callback_query(lambda c: c.data == "play_solo")
async def cb_play_solo(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    kb = []
    for b in BET_OPTIONS:
        kb.append(InlineKeyboardButton(text=f"{b} 💰", callback_data=f"start_solo_{b}"))
    kb = [kb]
    kb.append([InlineKeyboardButton(text="✏️ Своя ставка", callback_data="custom_bet")])
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="menu")])
    await call.message.edit_text(f"💰 Баланс: **{data['balance']}**\nВыберите ставку:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("start_solo_"))
async def cb_start_solo(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet:
        await call.answer("Недостаточно средств!", show_alert=True)
        return
    leave_all_tables(call.from_user.id)
    tid = str(uuid.uuid4())[:8]
    table = GameTable(tid, is_public=False, owner_id=call.from_user.id)
    tables[tid] = table
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet, data['balance'])
    table.start_game()
    txt = await render_table_for_player(table, p, bot)
    kb = get_game_kb(table, p)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id
    if table.state == "finished":
        await finalize_game_db(table)
        await update_table_messages(tid)

@dp.callback_query(lambda c: c.data == "custom_bet")
async def cb_custom_input(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("Введите сумму ставки:")
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
        p = table.add_player(message.from_user.id, message.from_user.first_name, bet, data['balance'])
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
        await message.answer("Введите целое число > 0")

@dp.callback_query(lambda c: c.data.startswith("replay_"))
async def cb_replay(call: CallbackQuery):
    tid = call.data.split("_")[1]
    await cb_play_solo(call)

# --- MULTIPLAYER & CHAT HANDLERS ---
@dp.callback_query(lambda c: c.data == "play_multi" or c.data == "refresh_multi")
async def cb_play_multi(call: CallbackQuery):
    waiting = [t for t in tables.values() if t.is_public and t.state == "waiting"]
    kb = []
    for t in waiting[:5]:
        owner = t.players[0].name if t.players else "Empty"
        kb.append([InlineKeyboardButton(text=f"Join {owner} ({len(t.players)}/{MAX_PLAYERS})", callback_data=f"prejoin_{t.id}")])
    kb.append([InlineKeyboardButton(text="➕ Создать стол", callback_data="create_table_setup")])
    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_multi")])
    kb.append([InlineKeyboardButton(text="🔙 Меню", callback_data="menu")])
    try: await call.message.edit_text("🌐 **Multiplayer Lobby**", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), parse_mode="Markdown")
    except: await call.answer()

@dp.callback_query(lambda c: c.data == "create_table_setup")
async def cb_create_setup(call: CallbackQuery):
    kb = []
    for b in BET_OPTIONS: kb.append([InlineKeyboardButton(text=f"{b} 💰", callback_data=f"new_multi_{b}")])
    kb.append([InlineKeyboardButton(text="🔙 Back", callback_data="play_multi")])
    await call.message.edit_text("Ставка для стола?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("new_multi_"))
async def cb_new_multi_created(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet:
        await call.answer("Мало денег!", show_alert=True); return
    leave_all_tables(call.from_user.id)
    tid = str(uuid.uuid4())[:5]
    table = GameTable(tid, is_public=True, owner_id=call.from_user.id)
    tables[tid] = table
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet, data['balance'])
    txt = render_lobby_table(table)
    kb = get_lobby_kb(table, p.userid)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id

@dp.callback_query(lambda c: c.data.startswith("prejoin_"))
async def cb_prejoin(call: CallbackQuery):
    tid = call.data.split("_")[1]
    t = tables.get(tid)
    if not t or t.state != "waiting": await call.answer("Стол недоступен"); return
    kb = [[InlineKeyboardButton(text=f"Join {b} 💰", callback_data=f"joinbet_{tid}_{b}")] for b in BET_OPTIONS]
    await call.message.edit_text(f"Ставка для входа в стол {tid}?", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(lambda c: c.data.startswith("joinbet_"))
async def cb_join_confirm(call: CallbackQuery):
    parts = call.data.split("_")
    tid, bet = parts[1], int(parts[2])
    t = tables.get(tid)
    if not t: await call.answer("Стол исчез"); return
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet: await call.answer("Нет денег"); return
    leave_all_tables(call.from_user.id)
    p = t.add_player(call.from_user.id, call.from_user.first_name, bet, data['balance'])
    txt = render_lobby_table(t)
    kb = get_lobby_kb(t, p.userid)
    msg = await call.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = msg.message_id
    await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("ready_"))
async def cb_ready(call: CallbackQuery):
    tid = call.data.split("_")[1]
    t = tables.get(tid)
    if not t: return
    p = t.get_player(call.from_user.id)
    if p:
        p.is_ready = True
        await call.answer("Готов!")
        if t.check_all_ready(): t.start_game()
        await update_table_messages(tid)

@dp.callback_query(lambda c: c.data.startswith("hit_"))
async def cb_hit(call: CallbackQuery):
    tid = call.data.split("_")[1]
    t = tables.get(tid)
    if not t: return
    p = t.get_player(call.from_user.id)
    if not p or t.players[t.current_player_index] != p: await call.answer("Не твой ход!"); return
    c, s = t.deck.get_card()
    if s: t.shuffle_alert = True
    p.hand.append(c)
    p.last_action = "hit"
    if p.value > 21: p.status = "bust"; t.process_turns()
    elif p.value == 21: p.status = "stand"; t.process_turns()
    if t.state == "finished": await finalize_game_db(t)
    await update_table_messages(tid)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("stand_"))
async def cb_stand(call: CallbackQuery):
    tid = call.data.split("_")[1]
    t = tables.get(tid)
    if not t: return
    p = t.get_player(call.from_user.id)
    if not p or t.players[t.current_player_index] != p: return
    p.status = "stand"
    p.last_action = "stand"
    t.process_turns()
    if t.state == "finished": await finalize_game_db(t)
    await update_table_messages(tid)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("double_"))
async def cb_double(call: CallbackQuery):
    tid = call.data.split("_")[1]
    t = tables.get(tid)
    if not t: return
    p = t.get_player(call.from_user.id)
    if not p or t.players[t.current_player_index] != p: return
    data = await get_player_data(p.userid)
    if data['balance'] < p.bet: await call.answer("Нет денег на удвоение!"); return
    p.bet *= 2
    c, s = t.deck.get_card()
    p.hand.append(c)
    p.last_action = "double"
    p.status = "stand" if p.value <= 21 else "bust"
    t.process_turns()
    if t.state == "finished": await finalize_game_db(t)
    await update_table_messages(tid)
    await call.answer()

@dp.message(F.text)
async def process_chat(message: types.Message, state: FSMContext):
    if await state.get_state(): return
    try: await message.delete()
    except: pass
    userid = message.from_user.id
    target = None
    for t in tables.values():
        if t.get_player(userid): target = t; break
    if target:
        target.add_chat_message(message.from_user.first_name, message.text)
        await update_table_messages(target.id)
        await log_chat(target.id, userid, message.from_user.username, message.text)

@dp.callback_query(lambda c: c.data == "stats")
async def cb_stats(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    s = data['stats']
    txt = (f"📊 **STATISTICS**\n\n"
           f"💰 Balance: {data['balance']}\n"
           f"🎮 Games: {s['games']}\n"
           f"🏆 Wins: {s['wins']}\n"
           f"💀 Losses: {s['losses']}\n"
           f"💎 Blackjacks: {s['blackjacks']}\n"
           f"📈 Max Balance: {s['maxbalance']}")
    await call.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙", callback_data="menu")]]), parse_mode="Markdown")

# --- БОНУС СИСТЕМА (НОВАЯ) ---
@dp.callback_query(lambda c: c.data == "free_chips")
async def cb_free_chips(call: CallbackQuery):
    user_id = call.from_user.id
    now_utc = datetime.now(timezone.utc)
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
            next_bonus_time = datetime.combine(current_bonus_day + timedelta(days=1), dt_time(6, 0), tzinfo=timezone.utc)
            delta = next_bonus_time - now_utc
            total_seconds = int(delta.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            
            await call.answer(f"⏳ Вы сегодня уже получили фишки\nДо следующего получения: {hours}ч {minutes}мин", show_alert=True)

# Команда для ручного фикса базы, если вдруг бонус не заработает
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
    asyncio.create_task(check_timeouts_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
