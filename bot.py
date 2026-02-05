import os
import asyncio
import random
import asyncpg
import uuid
import time
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
TURN_TIMEOUT = 30 # Секунд на ход

# ====== БАЗА ДАННЫХ ======
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
                max_balance INTEGER DEFAULT 1000,
                max_win INTEGER DEFAULT 0
            )
        """)
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS max_win INTEGER DEFAULT 0")
        except asyncpg.exceptions.DuplicateColumnError:
            pass
    print("Database initialized")

async def get_player_data(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not row:
            await conn.execute(
                "INSERT INTO users (user_id, balance, max_balance, max_win) VALUES ($1, $2, $2, 0) ON CONFLICT DO NOTHING",
                user_id, 1000
            )
            return {"balance": 1000, "stats": {"games":0, "wins":0, "losses":0, "pushes":0, "blackjacks":0, "max_balance":1000, "max_win":0}}
        
        return {
            "balance": row["balance"],
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
        blocks = int(percent * 10)
        bar = "▓" * blocks + "░" * (10 - blocks)
        return f"🎴 Колода: {bar} ({int(percent * 100)}%)"

class TablePlayer:
    def __init__(self, user_id, name, bet):
        self.user_id = user_id
        self.name = name
        self.bet = bet
        self.original_bet = bet
        self.hand = []
        self.status = "waiting" # waiting, playing, stand, bust, blackjack
        self.is_ready = False 
        self.message_id = None 

    @property
    def value(self):
        val = sum(10 if c[0] in "JQK" else 11 if c[0] == "A" else int(c[0]) for c in self.hand)
        aces = sum(1 for c in self.hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

    def render_hand(self):
        if not self.hand: return "(нет карт)"
        return "  ".join(f"`{r}{s}`" for r, s in self.hand)

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
        self.last_action_time = time.time() # Таймштамп активности

    def add_player(self, user_id, name, bet):
        player = TablePlayer(user_id, name, bet)
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
        self.update_activity() # Обновляем таймер при смене хода
        while self.current_player_index < len(self.players):
            p = self.players[self.current_player_index]
            if p.status == "playing":
                return # Ждем хода этого игрока
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

# ====== ФОНОВАЯ ЗАДАЧА: ПРОВЕРКА ТАЙМАУТОВ ======
async def check_timeouts_loop():
    while True:
        await asyncio.sleep(5) # Проверяем каждые 5 сек
        now = time.time()
        
        # Копируем values, чтобы не было ошибки изменения словаря во время итерации
        for table in list(tables.values()):
            # Проверяем только если сейчас ход игрока
            if table.state == "player_turn":
                if now - table.last_action_time > TURN_TIMEOUT:
                    # Время вышло!
                    try:
                        current_p = table.players[table.current_player_index]
                        current_p.status = "stand" # Принудительный Stand
                        
                        table.process_turns()
                        
                        if table.state == "finished":
                            await finalize_game_db(table)
                        
                        await update_table_messages(table.id)
                        
                        try: await bot.send_message(current_p.user_id, "⏳ Время хода вышло! Сработал авто-Stand.")
                        except: pass
                        
                    except IndexError:
                        pass # на всякий случай

# ====== ВИЗУАЛИЗАЦИЯ ======

def render_lobby(table: GameTable):
    txt = f"🎲 *Стол #{table.id}* (Ожидание)\n"
    txt += f"👥 Игроков: {len(table.players)}/{MAX_PLAYERS}\n\n"
    
    for i, p in enumerate(table.players, 1):
        role = "👑" if p.user_id == table.owner_id else "👤"
        status = "✅ Готов" if p.is_ready else "⏳ Ждем..."
        txt += f"{i}. {role} {p.name} — *{p.bet}* 🪙 ({status})\n"
    
    return txt

def get_lobby_kb(table: GameTable, user_id):
    kb = []
    p = table.get_player(user_id)
    
    # Кнопка изменения ставки (доступна пока не готов)
    if not p.is_ready:
        kb.append([InlineKeyboardButton(text="✅ Я ГОТОВ", callback_data=f"ready_{table.id}")])
        kb.append([InlineKeyboardButton(text="💰 Изм. ставку", callback_data=f"chbet_lobby_{table.id}")])
    else:
        # Если готов - можно отменить готовность через смену ставки или выход
        pass
    
    kb.append([InlineKeyboardButton(text="🚪 Выйти", callback_data=f"leave_lobby_{table.id}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def render_table_for_player(table: GameTable, player: TablePlayer, bot: Bot):
    if table.state == "finished":
        d_val = table._hand_value(table.dealer_hand)
        dealer_str = f"🤵 Дилер:  {'  '.join(f'`{r}{s}`' for r,s in table.dealer_hand)}  (*{d_val}*)"
    else:
        dealer_str = f"🤵 Дилер:  `{table.dealer_hand[0][0]}{table.dealer_hand[0][1]}`  `❓`"

    players_str = ""
    for p in table.players:
        marker = "⏳"
        if table.state == "player_turn":
            if table.players[table.current_player_index] == p:
                marker = f"👈 *ХОДИТ* ({TURN_TIMEOUT}с)"
            elif table.players.index(p) > table.current_player_index:
                marker = "💤"
            else:
                marker = ""
        
        name_display = "🧑 Ты" if p.user_id == player.user_id else f"👤 {p.name}"
        
        status_icon = ""
        if p.status == "blackjack": status_icon = "🃏 BJ!"
        elif p.status == "bust": status_icon = "💀 Перебор"
        elif p.status == "stand": status_icon = "✋"
        
        players_str += f"{name_display} ({p.bet}💰): {p.render_hand()} (*{p.value}*) {status_icon} {marker}\n"

    shoe = table.deck.get_visual_bar()
    shuffle_note = "\n\n_🔄 Колода перемешана_" if table.shuffle_alert else ""
    
    res_text = ""
    p_data = await get_player_data(player.user_id)
    balance_display = f"\n🪙 Баланс: *{p_data['balance']}*"
    
    if table.state == "finished":
        d_val = table._hand_value(table.dealer_hand)
        win = 0
        if player.status == "bust":
            res_text = "\n❌ *Перебор / Проигрыш*"
            win = -player.bet
        elif player.status == "blackjack":
            if d_val != 21 or len(table.dealer_hand) != 2:
                res_text = "\n🃏 *BLACKJACK! Победа!*"
                win = int(player.bet * 1.5)
            else:
                res_text = "\n🤝 *Ничья*"
                win = 0
        elif d_val > 21:
             res_text = "\n✅ *Дилер сгорел! Победа!*"
             win = player.bet
        elif player.value > d_val:
             res_text = "\n✅ *Победа!*"
             win = player.bet
        elif player.value < d_val:
             res_text = "\n❌ *Дилер выиграл*"
             win = -player.bet
        else:
             res_text = "\n🤝 *Ничья*"
             win = 0
             
        res_text += f" ({win:+})"
        
    text = (
        f"{dealer_str}\n"
        f"{players_str}\n"
        f"{shoe}{shuffle_note}"
        f"{res_text}"
        f"{balance_display}"
    )
    return text

def get_game_kb(table: GameTable, player: TablePlayer):
    if table.state == "finished":
        if not table.is_public:
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Играть еще", callback_data=f"replay_{table.id}")],
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
        stats = data['stats']
        bal = data['balance']
        
        win_amount = 0
        if p.status == "bust":
            win_amount = -p.bet
            stats['losses'] += 1
        elif p.status == "blackjack":
             win_amount = int(p.bet * 1.5)
             stats['wins'] += 1
             stats['blackjacks'] += 1
        elif d_val > 21 or p.value > d_val:
            win_amount = p.bet
            stats['wins'] += 1
        elif p.value < d_val:
            win_amount = -p.bet
            stats['losses'] += 1
        else:
            win_amount = 0
            stats['pushes'] += 1

        new_bal = bal + win_amount
        stats['games'] += 1
        stats['max_balance'] = max(stats['max_balance'], new_bal)
        if win_amount > 0: stats['max_win'] = max(stats['max_win'], win_amount)
            
        await update_player_stats(p.user_id, new_bal, stats)

# ====== ХЕНДЛЕРЫ ======

class BetState(StatesGroup):
    waiting = State()
    
class MultiCustomBet(StatesGroup):
    waiting = State()

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    data = await get_player_data(message.from_user.id)
    await message.answer(
        f"🃏 *Blackjack Revolution*\n🪙 Баланс: {data['balance']}",
        parse_mode="Markdown",
        reply_markup=main_menu_kb()
    )

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Одиночная игра", callback_data="play_solo")],
        [InlineKeyboardButton(text="👥 Онлайн столы", callback_data="play_multi")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
    ])

@dp.callback_query(lambda c: c.data == "menu")
async def cb_menu(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    await call.message.edit_text(f"🪙 Баланс: {data['balance']}", reply_markup=main_menu_kb())

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

    tid = str(uuid.uuid4())[:8]
    table = GameTable(tid, is_public=False, owner_id=call.from_user.id)
    tables[tid] = table
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet)
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
        # Запускаем соло игру с этой ставкой
        data = await get_player_data(message.from_user.id)
        if data['balance'] < bet:
            await message.answer("Недостаточно средств!")
            return
        
        tid = str(uuid.uuid4())[:8]
        table = GameTable(tid, is_public=False, owner_id=message.from_user.id)
        tables[tid] = table
        p = table.add_player(message.from_user.id, message.from_user.first_name, bet)
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
    # Фильтруем публичные столы, которые ждут игроков
    waiting_tables = [t for t in tables.values() if t.is_public and t.state == "waiting"]
    
    kb = []
    # Показываем столы с именем владельца
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

# Создание: фиксированная ставка
@dp.callback_query(lambda c: c.data.startswith("new_multi_"))
async def cb_new_multi_created(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    await create_multi_table(call, bet)

# Создание: кастомная ставка (Ввод)
@dp.callback_query(lambda c: c.data == "multi_custom_create")
async def cb_multi_custom_create_input(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("✍️ Введите ставку для стола:")
    await state.set_state(MultiCustomBet.waiting)
    await state.update_data(mode="create")

# Общая функция создания
async def create_multi_table(call: CallbackQuery, bet: int):
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet: return await call.answer("Не хватает денег!", show_alert=True)
    
    tid = str(uuid.uuid4())[:5]
    table = GameTable(tid, is_public=True, owner_id=call.from_user.id)
    tables[tid] = table
    
    p = table.add_player(call.from_user.id, call.from_user.first_name, bet)
    
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

# Присоединение: кастомная ставка (Ввод)
@dp.callback_query(lambda c: c.data.startswith("multi_custom_join_"))
async def cb_multi_custom_join_input(call: CallbackQuery, state: FSMContext):
    tid = call.data.split("_")[3]
    await call.message.edit_text(f"✍️ Введите ставку для входа (Стол #{tid}):")
    await state.set_state(MultiCustomBet.waiting)
    await state.update_data(mode="join", tid=tid)

# ОБРАБОТЧИК ВВОДА КАСТОМНОЙ СТАВКИ (МУЛЬТИПЛЕЕР)
@dp.message(MultiCustomBet.waiting)
async def process_multi_custom_bet(message: types.Message, state: FSMContext):
    try:
        bet = int(message.text)
        if bet <= 0: raise ValueError
        
        user_data = await state.get_data()
        mode = user_data.get("mode")
        
        # Проверяем баланс
        p_data = await get_player_data(message.from_user.id)
        if p_data['balance'] < bet:
            await message.answer("Недостаточно средств!")
            return
            
        if mode == "create":
            # Создаем стол (имитируем callback для удобства вызова create_multi_table, но проще создать вручную)
            tid = str(uuid.uuid4())[:5]
            table = GameTable(tid, is_public=True, owner_id=message.from_user.id)
            tables[tid] = table
            p = table.add_player(message.from_user.id, message.from_user.first_name, bet)
            
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
    # msg_obj может быть message или call, но здесь мы вызываем из message handler
    table = tables.get(tid)
    if not table or table.state != "waiting":
         return await msg_obj.answer("Стол исчез или игра началась.")

    p = table.add_player(msg_obj.from_user.id, msg_obj.from_user.first_name, bet)
    
    txt = render_lobby(table)
    kb = get_lobby_kb(table, p.user_id)
    sent_msg = await msg_obj.answer(txt, reply_markup=kb, parse_mode="Markdown")
    p.message_id = sent_msg.message_id
    
    await update_table_messages(tid)

# -- 3. Присоединение к столу (Вход фикс) --
@dp.callback_query(lambda c: c.data.startswith("joinbet_"))
async def cb_join_confirm(call: CallbackQuery):
    parts = call.data.split("_") 
    tid = parts[1]
    bet = int(parts[2])
    
    table = tables.get(tid)
    if not table or table.state != "waiting":
         return await call.message.edit_text("Стол исчез или игра началась.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Ок", callback_data="play_multi")]]))
    
    data = await get_player_data(call.from_user.id)
    if data['balance'] < bet:
        return await call.answer("Не хватает денег!", show_alert=True)

    p = table.add_player(call.from_user.id, call.from_user.first_name, bet)
    
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
    
    stats_text = (
        f"📊 *Личная статистика*\n\n"
        f"🎮 Игры: *{s['games']}*\n"
        f"🏆 Победы: *{s['wins']}*\n"
        f"💀 Поражения: *{s['losses']}*\n"
        f"🤝 Ничьи: *{s['pushes']}*\n"
        f"🃏 Blackjack: *{s['blackjacks']}*\n"
        f"📈 Win Rate: *{win_rate}%*\n\n"
        f"🪙 Баланс: *{data['balance']}*\n"
        f"🏦 Макс. баланс: *{s['max_balance']}*\n"
        f"🤑 Макс. выигрыш: *{s['max_win']}*\n\n"
        f"🆔 ID: `{call.from_user.id}`"
    )
    
    await call.message.edit_text(
        stats_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Меню", callback_data="menu")]])
    )

async def main():
    await init_db()
    print("Bot started")
    asyncio.create_task(check_timeouts_loop()) # Запуск фоновой проверки таймаутов
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
