import os
import asyncio
import random
import asyncpg
import uuid
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
            return self.shoe.pop(), True # True = перемешано
        return self.shoe.pop(), False

    def get_visual_bar(self):
        percent = len(self.shoe) / TOTAL_CARDS
        blocks = int(percent * 10)
        bar = "▓" * blocks + "░" * (10 - blocks)
        return f"🎴 Колода: {bar} ({int(percent * 100)}%)"

# Глобальная колода для столов (можно сделать по одной на стол, сделаем пока одну общую для экономии памяти, 
# либо в будущем перенесем внутрь Table)
# Для честности лучше: 1 Стол = 1 Колода. Сделаем так.

class TablePlayer:
    def __init__(self, user_id, name, bet):
        self.user_id = user_id
        self.name = name
        self.bet = bet
        self.original_bet = bet
        self.hand = []
        self.status = "playing" # playing, stand, bust, blackjack, surrender
        self.message_id = None # ID сообщения чата, чтобы его обновлять

    @property
    def value(self):
        val = sum(10 if c[0] in "JQK" else 11 if c[0] == "A" else int(c[0]) for c in self.hand)
        aces = sum(1 for c in self.hand if c[0] == "A")
        while val > 21 and aces:
            val -= 10
            aces -= 1
        return val

    def render_hand(self):
        return "  ".join(f"`{r}{s}`" for r, s in self.hand)

class GameTable:
    def __init__(self, table_id, is_public=False):
        self.id = table_id
        self.is_public = is_public
        self.players = [] # Список объектов TablePlayer
        self.dealer_hand = []
        self.deck = CardSystem()
        self.state = "waiting" # waiting, dealing, player_turn, dealer_turn, finished
        self.current_player_index = 0
        self.shuffle_alert = False

    def add_player(self, user_id, name, bet):
        player = TablePlayer(user_id, name, bet)
        self.players.append(player)
        return player

    def get_player(self, user_id):
        for p in self.players:
            if p.user_id == user_id:
                return p
        return None

    def deal_initial(self):
        self.dealer_hand = []
        self.shuffle_alert = False
        
        # Дилеру
        c, s = self.deck.get_card()
        if s: self.shuffle_alert = True
        self.dealer_hand.append(c)
        
        c, s = self.deck.get_card()
        if s: self.shuffle_alert = True
        self.dealer_hand.append(c)

        # Игрокам
        for p in self.players:
            p.hand = []
            c1, s1 = self.deck.get_card()
            c2, s2 = self.deck.get_card()
            p.hand = [c1, c2]
            if s1 or s2: self.shuffle_alert = True
            
            # Проверка на Блэкджек сразу
            if p.value == 21:
                p.status = "blackjack"
        
        self.state = "player_turn"
        self.current_player_index = 0
        self.process_turns() # Проверяем, может первый уже выиграл или все выиграли

    def process_turns(self):
        # Ищем следующего игрока, который 'playing'
        while self.current_player_index < len(self.players):
            p = self.players[self.current_player_index]
            if p.status == "playing":
                return # Нашли, ждем его хода
            self.current_player_index += 1
        
        # Если игроков не осталось -> ход дилера
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

# Глобальное хранилище столов
tables = {} # table_id -> GameTable

# ====== ХЕЛПЕРЫ ДЛЯ ОТОБРАЖЕНИЯ ======

async def render_table_for_player(table: GameTable, player: TablePlayer, bot: Bot):
    # Дилер
    if table.state == "finished":
        d_val = table._hand_value(table.dealer_hand)
        dealer_str = f"🤵 Дилер:  {'  '.join(f'`{r}{s}`' for r,s in table.dealer_hand)}  (*{d_val}*)"
    else:
        dealer_str = f"🤵 Дилер:  `{table.dealer_hand[0][0]}{table.dealer_hand[0][1]}`  `❓`"

    # Игроки (строим список)
    players_str = ""
    for p in table.players:
        marker = "👈" if (table.state == "player_turn" and table.players[table.current_player_index] == p) else ""
        if p.user_id == player.user_id:
            name_display = "🧑 Ты"
        else:
            name_display = f"👤 {p.name}"
        
        status_icon = ""
        if p.status == "blackjack": status_icon = "🃏 BJ!"
        elif p.status == "bust": status_icon = "💀 Перебор"
        elif p.status == "stand": status_icon = "✋"
        
        players_str += f"{name_display}: {p.render_hand()} (*{p.value}*) {status_icon} {marker}\n"

    # Подвал
    shoe = table.deck.get_visual_bar()
    shuffle_note = "\n\n_🔄 Колода перемешана_" if table.shuffle_alert else ""
    
    # Результат (если конец)
    res_text = ""
    if table.state == "finished":
        d_val = table._hand_value(table.dealer_hand)
        
        # Расчет для КОНКРЕТНОГО игрока, кому рендерим
        win = 0
        if player.status == "bust":
            res_text = "\n❌ *Перебор / Проигрыш*"
            win = -player.bet
        elif player.status == "blackjack":
             # Если у дилера не BJ
            if d_val != 21 or len(table.dealer_hand) != 2: # Упрощенно, в казино проверка дилера BJ сложнее
                res_text = "\n🃏 *BLACKJACK! Победа!*"
                win = int(player.bet * 1.5)
            else:
                res_text = "\n🤝 *Ничья (BJ против BJ)*"
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
             
        # Обновляем БД (только один раз делаем это в контроллере, здесь просто отображение)
        # ВАЖНО: Мы не пишем в БД в функции рендера!
        
        res_text += f" ({win:+})"

    text = (
        f"💰 Ставка: *{player.bet}*\n\n"
        f"{dealer_str}\n"
        f"{players_str}\n"
        f"{shoe}{shuffle_note}"
        f"{res_text}"
    )
    return text

def get_game_kb(table: GameTable, player: TablePlayer):
    # Если конец игры
    if table.state == "finished":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Еще раз", callback_data=f"replay_{table.id}_{player.original_bet}"),
             InlineKeyboardButton(text="🚪 Меню", callback_data="menu")]
        ])

    # Если не ход этого игрока
    current_p = table.players[table.current_player_index]
    if current_p != player:
        return None # Нет кнопок, если не твой ход

    # Если ход игрока
    can_double = True # Проверку баланса добавим в хендлер
    
    kb = [
        [InlineKeyboardButton(text="🖐 HIT", callback_data=f"hit_{table.id}"),
         InlineKeyboardButton(text="✋ STAND", callback_data=f"stand_{table.id}")]
    ]
    if len(player.hand) == 2 and can_double:
        kb.insert(0, [InlineKeyboardButton(text="2️⃣ x2", callback_data=f"double_{table.id}")])
    
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ====== ЛОГИКА ОБНОВЛЕНИЯ (BROADCAST) ======
async def update_table_messages(table_id):
    table = tables.get(table_id)
    if not table: return

    for p in table.players:
        if p.message_id:
            txt = await render_table_for_player(table, p, bot)
            kb = get_game_kb(table, p)
            try:
                # Если текст и клавиатура не изменились, телеграм кинет ошибку, игнорим её
                await bot.edit_message_text(txt, chat_id=p.user_id, message_id=p.message_id, reply_markup=kb, parse_mode="Markdown")
            except TelegramBadRequest:
                pass
            except Exception as e:
                print(f"Error update msg: {e}")

async def finalize_game_db(table: GameTable):
    # Сохраняем результаты в БД для всех
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
             # Проверка BJ дилера для простоты опустим (считаем BJ игрока победой 3:2 всегда, если не PUSH)
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
        if win_amount > 0:
            stats['max_win'] = max(stats['max_win'], win_amount)
            
        await update_player_stats(p.user_id, new_bal, stats)

# ====== ХЕНДЛЕРЫ ======

# FSM
class BetState(StatesGroup):
    waiting = State()

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    data = await get_player_data(message.from_user.id)
    await message.answer(
        f"🃏 *Blackjack Revolution*\n"
        f"Теперь на новом движке!\n\n"
        f"🪙 Баланс: {data['balance']}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Одиночная игра", callback_data="play_solo")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
        ])
    )

@dp.callback_query(lambda c: c.data == "menu")
async def cb_menu(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    await call.message.edit_text(
        f"🪙 Баланс: {data['balance']}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Одиночная игра", callback_data="play_solo")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")]
        ])
    )

@dp.callback_query(lambda c: c.data == "play_solo")
async def cb_play_solo(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    kb = [[InlineKeyboardButton(text=f"💰 {b}", callback_data=f"start_solo_{b}")] for b in BET_OPTIONS]
    kb.append([InlineKeyboardButton(text="✍️ Своя ставка", callback_data="custom_bet")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="menu")])
    
    await call.message.edit_text(f"🪙 Баланс: {data['balance']}\nВыберите ставку:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# -- Запуск Соло Игры --
async def start_solo_game(user_id, name, bet, message_entity):
    # 1. Проверка баланса
    data = await get_player_data(user_id)
    if data['balance'] < bet:
        await message_entity.answer("❌ Недостаточно средств!")
        return

    # 2. Создаем стол
    table_id = str(uuid.uuid4())[:8]
    table = GameTable(table_id, is_public=False)
    player = table.add_player(user_id, name, bet)
    tables[table_id] = table
    
    # 3. Раздача
    table.deal_initial()
    
    # 4. Отправка сообщения
    txt = await render_table_for_player(table, player, bot)
    kb = get_game_kb(table, player)
    
    if isinstance(message_entity, types.CallbackQuery):
        msg = await message_entity.message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
        player.message_id = msg.message_id
    else:
        msg = await message_entity.answer(txt, reply_markup=kb, parse_mode="Markdown")
        player.message_id = msg.message_id

    # 5. Если сразу конец (BJ у всех)
    if table.state == "finished":
        await finalize_game_db(table)
        await update_table_messages(table_id) # обновить, чтобы показать результат

@dp.callback_query(lambda c: c.data.startswith("start_solo_"))
async def cb_start_solo_bet(call: CallbackQuery):
    bet = int(call.data.split("_")[2])
    await start_solo_game(call.from_user.id, call.from_user.first_name, bet, call)

@dp.callback_query(lambda c: c.data.startswith("replay_"))
async def cb_replay(call: CallbackQuery):
    # replay_tableid_bet
    parts = call.data.split("_")
    bet = int(parts[2])
    # Старый стол удаляем из памяти, создаем новый
    await start_solo_game(call.from_user.id, call.from_user.first_name, bet, call)

# -- Игровые действия --

@dp.callback_query(lambda c: c.data.startswith("hit_"))
async def cb_hit(call: CallbackQuery):
    table_id = call.data.split("_")[1]
    table = tables.get(table_id)
    if not table: return await call.answer("Стол закрыт")

    player = table.get_player(call.from_user.id)
    if not player or table.players[table.current_player_index] != player:
        return await call.answer("Не твой ход!")

    # Логика Hit
    c, s = table.deck.get_card()
    if s: table.shuffle_alert = True
    player.hand.append(c)
    
    if player.value > 21:
        player.status = "bust"
        await call.answer("Перебор!", show_alert=False)
        table.process_turns() # переход хода
    elif player.value == 21:
        player.status = "stand"
        await call.answer("21! Стоп.", show_alert=False)
        table.process_turns()
    
    # Если стол завершился
    if table.state == "finished":
        await finalize_game_db(table)
    
    await update_table_messages(table_id)

@dp.callback_query(lambda c: c.data.startswith("stand_"))
async def cb_stand(call: CallbackQuery):
    table_id = call.data.split("_")[1]
    table = tables.get(table_id)
    if not table: return await call.answer("Стол закрыт")

    player = table.get_player(call.from_user.id)
    if not player or table.players[table.current_player_index] != player:
        return await call.answer("Не твой ход!")

    player.status = "stand"
    await call.answer("Стоп.")
    table.process_turns() # Переход хода
    
    if table.state == "finished":
        await finalize_game_db(table)
    
    await update_table_messages(table_id)

@dp.callback_query(lambda c: c.data.startswith("double_"))
async def cb_double(call: CallbackQuery):
    table_id = call.data.split("_")[1]
    table = tables.get(table_id)
    if not table: return 
    player = table.get_player(call.from_user.id)
    
    # Проверка баланса
    data = await get_player_data(player.user_id)
    if data['balance'] < player.bet * 2:
        return await call.answer("Не хватает фишек!", show_alert=True)
    
    player.bet *= 2
    c, s = table.deck.get_card()
    if s: table.shuffle_alert = True
    player.hand.append(c)
    
    if player.value > 21:
        player.status = "bust"
    else:
        player.status = "stand" # После дабла всегда стенд
        
    await call.answer(f"Удвоение! Ставка: {player.bet}")
    table.process_turns()
    
    if table.state == "finished":
        await finalize_game_db(table)
    
    await update_table_messages(table_id)

# -- Статистика --
@dp.callback_query(lambda c: c.data == "stats")
async def cb_stats(call: CallbackQuery):
    data = await get_player_data(call.from_user.id)
    s = data['stats']
    rate = round(s['wins']/s['games']*100, 1) if s['games'] else 0
    txt = (
        f"📊 *Статистика*\n"
        f"Игр: {s['games']} | Побед: {s['wins']} ({rate}%)\n"
        f"Баланс: {data['balance']}\n"
        f"Макс. выигрыш: {s['max_win']}\n"
        f"ID: `{call.from_user.id}`"
    )
    await call.message.edit_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Меню", callback_data="menu")]]))

# -- Кастомная ставка --
@dp.callback_query(lambda c: c.data == "custom_bet")
async def cb_custom_input(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("✍️ Введите ставку:")
    await state.set_state(BetState.waiting)

@dp.message(BetState.waiting)
async def process_custom_bet(message: types.Message, state: FSMContext):
    try:
        bet = int(message.text)
        if bet <= 0: raise ValueError
        await start_solo_game(message.from_user.id, message.from_user.first_name, bet, message)
        await state.clear()
    except:
        await message.answer("Ошибка. Введите целое число > 0")

async def main():
    await init_db()
    print("Bot started (New Engine)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
