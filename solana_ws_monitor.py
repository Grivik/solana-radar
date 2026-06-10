import os
import datetime
import asyncio
import json
import csv
import time
import base64
import websockets
import ssl
import aiohttp
from dotenv import load_dotenv
import websockets.exceptions
from aiogram import Bot
from aioconsole import ainput  # асинхронный ввод с консоли

# Загружаем настройки из .env
load_dotenv()

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
MIN_SWAP_AMOUNT_SOL = float(os.getenv("MIN_SWAP_AMOUNT_SOL", "0.01"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "1000.0"))  # порог ликвидности токена в USD

# Параметры торговли
ENABLE_SEMI_AUTO = os.getenv("ENABLE_SEMI_AUTO", "False").lower() == "true"  # полуавтоматический режим покупки
FULL_AUTO = os.getenv("FULL_AUTO", "False").lower() == "true"  # полностью автоматический режим
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "10.0"))   # тейк-профит в процентах
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "10.0"))       # стоп-лосс в процентах
MAX_BUY_SOL = float(os.getenv("MAX_BUY_SOL", "0.5"))  # максимальная сумма одной покупки в SOL
TRADING_MODE = os.getenv("TRADING_MODE", "paper")  # "paper" или "live"

# Лимиты позиций и тайм-аут удержания
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
CASCADE_TP_MINUTES = int(os.getenv("CASCADE_TP_MINUTES", "15"))
CASCADE_BE_MINUTES = int(os.getenv("CASCADE_BE_MINUTES", "20"))
HARD_TIMEOUT_MINUTES = int(os.getenv("HARD_TIMEOUT_MINUTES", "30"))

# Комиссии и проскальзывание
SIM_FEE_PCT = float(os.getenv("SIM_FEE_PCT", "0.3"))          # комиссия симулятора в процентах
MAX_SLIPPAGE_PCT = float(os.getenv("MAX_SLIPPAGE_PCT", "5.0")) # допустимое проскальзывание (пока не используется, для лога)

SIM_TRADES_FILE = os.getenv("SIM_TRADES_FILE", "sim_trades.csv")  # путь к файлу симулятора сделок
SIM_BALANCE_FILE = os.getenv("SIM_BALANCE_FILE", "sim_balance.txt")  # файл сохранения виртуального баланса
SIM_PORTFOLIO_FILE = os.getenv("SIM_PORTFOLIO_FILE", "sim_portfolio.json")  # файл сохранения портфеля
SIM_BALANCE_SOL = float(os.getenv("SIM_BALANCE_SOL", "100.0"))    # стартовый виртуальный баланс
virtual_balance = SIM_BALANCE_SOL                                 # текущий виртуальный баланс

# Восстановление виртуального баланса из файла, если он существует
if os.path.exists(SIM_BALANCE_FILE):
    try:
        with open(SIM_BALANCE_FILE, 'r') as f:
            virtual_balance = float(f.read().strip())
        print(f"[OK] Восстановлен виртуальный баланс: {virtual_balance:.4f} SOL")
    except Exception:
        print("[WARNING] Не удалось восстановить баланс, используется начальное значение.")

# Портфель: ключ – адрес токена, значение – словарь с полями symbol, name, amount, buy_price, entry_time
portfolio = {}

# Настройки Rugcheck
RUGCHECK_ENABLED = os.getenv("RUGCHECK_ENABLED", "False").lower() == "true"
RUGCHECK_CACHE_MINUTES = int(os.getenv("RUGCHECK_CACHE_MINUTES", "5"))
rugcheck_cache = {}  # ключ: token_address, значение: {'status': bool/None, 'timestamp': float}

# Фильтр возраста пары через DexScreener
MIN_PAIR_AGE_MINUTES = int(os.getenv("MIN_PAIR_AGE_MINUTES", "15"))
REQUIRE_DEXSCREENER_INFO = os.getenv("REQUIRE_DEXSCREENER_INFO", "True").lower() == "true"
MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "100.0"))  # минимальный объём торгов за 5 минут

# Настройки Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not HELIUS_API_KEY:
    raise ValueError("HELIUS_API_KEY не найден в .env файле")

# Инициализируем бота, если настройки заданы
bot = None
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    print("[OK] Настройки Telegram успешно загружены.")
else:
    print("[WARNING] Настройки Telegram не заданы. Работа будет продолжена без уведомлений.")

WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HTTP_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

ssl._create_default_https_context = ssl._create_unverified_context

# ID программы Raydium Liquidity Pool V4 (для извлечения адреса токена)
RAYDIUM_LIQUIDITY_POOL_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"


def save_balance(balance):
    """Сохраняет текущий виртуальный баланс в файл."""
    try:
        with open(SIM_BALANCE_FILE, 'w') as f:
            f.write(str(balance))
    except Exception as e:
        print(f"[WARNING] Не удалось сохранить баланс: {e}")


def save_portfolio(portfolio_dict):
    """Сохраняет словарь портфеля в JSON-файл."""
    try:
        with open(SIM_PORTFOLIO_FILE, 'w') as f:
            json.dump(portfolio_dict, f)
    except Exception as e:
        print(f"[WARNING] Не удалось сохранить портфель: {e}")


def load_portfolio():
    """Загружает портфель из JSON-файла. Возвращает словарь, либо пустой словарь при ошибке."""
    if os.path.exists(SIM_PORTFOLIO_FILE):
        try:
            with open(SIM_PORTFOLIO_FILE, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            print(f"[WARNING] Ошибка загрузки портфеля: {e}")
    return {}


async def check_rugcheck(session, token_address):
    """
    Проверяет токен через API rugcheck.xyz.
    Возвращает:
        True  – безопасен (нет опасных рисков)
        False – найден риск уровня "risky" или "malicious"
        None  – не удалось выполнить проверку
    Использует кэш с временем жизни RUGCHECK_CACHE_MINUTES.
    """
    global rugcheck_cache

    # Проверка кэша
    cached = rugcheck_cache.get(token_address)
    if cached and (time.time() - cached['timestamp']) < RUGCHECK_CACHE_MINUTES * 60:
        return cached['status']

    url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                risks = data.get('risks', [])
                if not risks:
                    status = True
                else:
                    # Ищем хотя бы один риск с level "risky" или "malicious"
                    dangerous = any(r.get('level') in ('risky', 'malicious') for r in risks)
                    status = not dangerous
            else:
                status = None
    except Exception as e:
        print(f"[RUGCHECK] Ошибка запроса для {token_address}: {e}")
        status = None

    # Сохраняем в кэш
    rugcheck_cache[token_address] = {'status': status, 'timestamp': time.time()}
    return status


async def fetch_pair_info(session, token_address):
    """
    Получает возраст пары и объём торгов через DexScreener API.
    Возвращает словарь {'age_minutes': float или None, 'volume_5m': float или None}.
    Если пар нет или ошибка — возвращает None для обоих полей.
    """
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get('pairs', [])
                if pairs:
                    p = pairs[0]
                    # Возраст пары
                    pair_created_at_ms = p.get('pairCreatedAt')
                    age_minutes = None
                    if pair_created_at_ms:
                        age_seconds = time.time() - (pair_created_at_ms / 1000.0)
                        age_minutes = age_seconds / 60.0
                    # Объём за 5 минут
                    volume_5m = p.get('volume', {}).get('m5', None)
                    return {'age_minutes': age_minutes, 'volume_5m': volume_5m}
    except Exception as e:
        print(f"[DEXSCREENER] Ошибка запроса для {token_address}: {e}")
    return {'age_minutes': None, 'volume_5m': None}


async def send_tg_notification(text):
    """Асинхронная отправка сообщения в Telegram без блокировки основного цикла."""
    if not bot:
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, disable_web_page_preview=False)
    except Exception as e:
        print(f"[TG ERROR] Не удалось отправить уведомление: {e}")


async def fetch_tx_details(session, signature):
    """Получение деталей транзакции через Helius HTTP API."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    try:
        async with session.post(HTTP_URL, json=payload, timeout=5) as response:
            if response.status == 200:
                result = await response.json()
                return result.get('result')
    except Exception as e:
        print(f"[ERROR] Не удалось получить детали TX {signature}: {e}")
    return None

async def get_jupiter_quote(session, input_mint, output_mint, amount_sol):
    """
    Получает котировку от Jupiter API для обмена SOL -> токен.
    Возвращает словарь с маршрутом обмена или None при ошибке.
    """
    url = "https://quote-api.jup.ag/v6/quote"
    params = {
        "inputMint": input_mint,          # SOL
        "outputMint": output_mint,        # адрес токена
        "amount": int(amount_sol * 1e9),  # переводим SOL в лампорты
        "slippageBps": int(MAX_SLIPPAGE_PCT * 100),  # из .env, в базисных пунктах
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "true"
    }
    try:
        async with session.get(url, params=params, timeout=5) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                print(f"[JUPITER] Ошибка quote: статус {resp.status}")
                return None
    except Exception as e:
        print(f"[JUPITER] Ошибка запроса quote: {e}")
        return None

async def execute_jupiter_swap(session, token_address, token_symbol, change_sol):
    """
    Выполняет реальный своп SOL -> токен через Jupiter API.
    Возвращает True при успехе, False при ошибке.
    """
    # Загружаем приватный ключ
    private_key_str = os.getenv("SOLANA_PRIVATE_KEY", "")
    if not private_key_str:
        print("[JUPITER] Приватный ключ не задан. Своп не выполнен.")
        return False

    # Получаем котировку
    quote = await get_jupiter_quote(
        session,
        "So11111111111111111111111111111111111111112",  # mint адрес SOL
        token_address,
        change_sol
    )
    if not quote:
        print(f"[JUPITER] Не удалось получить котировку для {token_symbol}")
        return False

    # Выводим информацию о котировке
    print(f"[JUPITER] Котировка для {token_symbol}:")
    print(f"  Вход: {change_sol:.4f} SOL")
    out_amount = int(quote.get('outAmount', 0)) / 1e9
    print(f"  Выход: {out_amount:.6f} токенов")
    print(f"  Price Impact: {quote.get('priceImpactPct', 'N/A')}%")

    # Получаем готовую транзакцию от Jupiter (swap transaction)
    tx_payload = {
        "quoteResponse": quote,
        "userPublicKey": "DF2RCLMsyp3maQyjDrpJtCDGvX2R8aqEmGyEZVsKXLqB",
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": 100000  # приоритетная комиссия
    }
    try:
        async with session.post("https://quote-api.jup.ag/v6/swap", json=tx_payload, timeout=10) as resp:
            if resp.status != 200:
                print(f"[JUPITER] Ошибка получения транзакции: статус {resp.status}")
                return False
            swap_data = await resp.json()
            raw_tx = swap_data.get('swapTransaction')
            if not raw_tx:
                print("[JUPITER] Пустая транзакция в ответе swap")
                return False
    except Exception as e:
        print(f"[JUPITER] Ошибка запроса swap: {e}")
        return False

    # Подписываем транзакцию
    try:
        from solders.keypair import Keypair
        from solders.transaction import VersionedTransaction
        from base64 import b64decode

        keypair = Keypair.from_base58_string(private_key_str)
        tx_bytes = b64decode(raw_tx)
        tx = VersionedTransaction.from_bytes(tx_bytes)
        tx.sign([keypair])
        signed_tx = tx
    except Exception as e:
        print(f"[JUPITER] Ошибка подписи транзакции: {e}")
        return False

    # Отправляем транзакцию в сеть
    try:
        send_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                base64.b64encode(bytes(signed_tx)).decode('utf-8'),
                {"encoding": "base64", "maxRetries": 3}
            ]
        }
        async with session.post(HTTP_URL, json=send_payload, timeout=10) as resp:
            if resp.status != 200:
                print(f"[JUPITER] Ошибка отправки транзакции: статус {resp.status}")
                return False
            result = await resp.json()
            tx_id = result.get('result', 'N/A')
            print(f"[JUPITER] Транзакция отправлена! TX ID: {tx_id}")
            print(f"[JUPITER] Проверить: https://solscan.io/tx/{tx_id}")
            return True
    except Exception as e:
        print(f"[JUPITER] Ошибка отправки транзакции: {e}")
        return False

async def fetch_token_info(session, token_address):
    """
    Получение информации о токене через Helius DAS API (getAsset).
    Возвращает словарь с ключами:
        name, symbol, supply, price_per_token
    или None в случае ошибки/отсутствия данных.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": [token_address]
    }
    try:
        async with session.post(HTTP_URL, json=payload, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                result = data.get('result')
                if not result:
                    return None
                # Извлечение базовых параметров
                token_info = result.get('token_info', {})
                supply = token_info.get('supply', 0)
                if supply is None:
                    supply = 0
                price_info = token_info.get('price_info', {})
                price_per_token = price_info.get('price_per_token', 0)
                if price_per_token is None:
                    price_per_token = 0

                # Извлечение имени и символа из метаданных
                content = result.get('content', {})
                metadata = content.get('metadata', {})
                name = metadata.get('name', 'N/A')
                symbol = metadata.get('symbol', 'N/A')

                return {
                    'name': name,
                    'symbol': symbol,
                    'supply': supply,
                    'price_per_token': price_per_token
                }
    except Exception as e:
        print(f"[WARNING] Ошибка получения информации о токене {token_address}: {e}")
    return None


def extract_token_from_tx(tx_data):
    """
    Извлекает адрес токена (Mint), вовлечённого в своп,
    на основе изменения токенного баланса подписанта.
    Анализирует meta.preTokenBalances / postTokenBalances.
    Возвращает адрес Mint или None.
    """
    try:
        meta = tx_data.get('meta', {})
        account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
        if not account_keys:
            return None
        signer = account_keys[0]['pubkey']  # извлекаем строку pubkey первого аккаунта

        pre_token_balances = meta.get('preTokenBalances', [])
        post_token_balances = meta.get('postTokenBalances', [])

        # Ищем запись, относящуюся к подписанту
        for entry in pre_token_balances + post_token_balances:
            if entry.get('owner') == signer:
                mint = entry.get('mint')
                if mint:
                    return mint
    except Exception:
        pass
    return None


async def ask_confirmation(timeout=30):
    """
    Запрашивает подтверждение на покупку у пользователя.
    Возвращает:
        True  – если введено 'y'
        False – если введено 'n'
        None  – при таймауте или нераспознанном вводе
    """
    try:
        user_input = await asyncio.wait_for(ainput("Купить? (y/n): "), timeout=timeout)
        answer = user_input.strip().lower()
        if answer == 'y':
            return True
        elif answer == 'n':
            return False
        else:
            print("[WARNING] Нераспознанный ввод, ожидалось 'y' или 'n'.")
            return None
    except asyncio.TimeoutError:
        print("\n[WARNING] Время ожидания подтверждения истекло.")
        return None
    except Exception as e:
        print(f"[ERROR] Ошибка ввода: {e}")
        return None


async def write_sim_trade(trade_data, new_balance, side, pnl_sol):
    """
    Добавляет запись о симулированной сделке в CSV-файл.
    Формат: timestamp,token_address,symbol,name,side,sol_amount,price_per_token,liquidity_usd,balance_after,pnl_sol
    """
    try:
        import datetime
        utc_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        row = [
            utc_time,
            trade_data['token_address'],
            trade_data['symbol'],
            trade_data['name'],
            side,
            f"{trade_data['sol_amount']:.6f}",
            f"{trade_data['price_per_token']:.6f}",
            f"{trade_data['liquidity_usd']:.2f}",
            f"{new_balance:.6f}",
            f"{pnl_sol:.6f}"
        ]

        file_exists = os.path.isfile(SIM_TRADES_FILE)
        with open(SIM_TRADES_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists or os.stat(SIM_TRADES_FILE).st_size == 0:
                writer.writerow(['timestamp', 'token_address', 'symbol', 'name', 'side',
                                 'sol_amount', 'price_per_token', 'liquidity_usd',
                                 'balance_after', 'pnl_sol'])
            writer.writerow(row)
    except Exception as e:
        print(f"[ERROR] Не удалось записать симуляцию сделки в CSV: {e}")


async def check_hold_timeout(session):
    """
    Каскадный тайм-аут: проверяет позиции по трём уровням:
    - CASCADE_TP минут: продажа при любой прибыли (pnl > 0)
    - CASCADE_BE минут: продажа при pnl > -0.00001 (почти безубыток)
    - HARD_TIMEOUT минут: продажа в любом случае
    """
    global virtual_balance, portfolio

    if not portfolio:
        return

    for token_address in list(portfolio.keys()):
        try:
            pos = portfolio[token_address]
            if 'entry_time' not in pos:
                print(f"[TIMEOUT] У позиции {pos.get('symbol', token_address)} нет entry_time, пропущена.")
                continue
            age_minutes = (time.time() - pos['entry_time']) / 60.0

            should_sell = False
            reason = ""

            # Получаем текущую цену для расчёта PnL
            token_info = await fetch_token_info(session, token_address)
            if not token_info:
                print(f"[TIMEOUT] Не удалось получить цену для {pos['symbol']}, пропускаем.")
                continue
            current_price = token_info['price_per_token']
            if current_price <= 0:
                continue

            buy_price = pos['buy_price']
            price_change_pct = (current_price - buy_price) / buy_price * 100
            amount = pos['amount']
            pnl_sol = amount * (price_change_pct / 100)

            # Каскадная логика
                        # Ступенчатый каскадный тайм-аут (строгие временные окна)
            if age_minutes >= HARD_TIMEOUT_MINUTES:
                should_sell = True
                reason = f"жёсткий тайм-аут ({HARD_TIMEOUT_MINUTES} мин)"
            elif CASCADE_BE_MINUTES <= age_minutes < HARD_TIMEOUT_MINUTES:
                if pnl_sol > -0.00001:
                    should_sell = True
                    reason = f"каскадный выход: почти безубыток ({CASCADE_BE_MINUTES}-{HARD_TIMEOUT_MINUTES} мин)"
            elif CASCADE_TP_MINUTES <= age_minutes < CASCADE_BE_MINUTES:
                if pnl_sol > 0:
                    should_sell = True
                    reason = f"каскадный выход: микро-прибыль ({CASCADE_TP_MINUTES}-{CASCADE_BE_MINUTES} мин)"

            if should_sell:
                virtual_balance += amount + pnl_sol
                save_balance(virtual_balance)

                sim_trade = {
                    'timestamp': time.time(),
                    'token_address': token_address,
                    'symbol': pos['symbol'],
                    'name': pos['name'],
                    'sol_amount': amount,
                    'price_per_token': current_price,
                    'liquidity_usd': 0.0
                }
                await write_sim_trade(sim_trade, virtual_balance, side='SELL', pnl_sol=pnl_sol)

                emoji = "✅" if pnl_sol >= 0 else "❌"
                print(f">>> [TIMEOUT] Принудительная продажа: {pos['symbol']}, "
                      f"удерживалась {age_minutes:.1f} мин, причина: {reason}, "
                      f"PnL: {pnl_sol:+.4f} SOL, Баланс: {virtual_balance:.4f} SOL {emoji}")

                del portfolio[token_address]
                save_portfolio(portfolio)
        except Exception as e:
            print(f"[TIMEOUT] Ошибка при обработке {token_address}: {e}")

async def monitor(ws, session):
    """Обработка потока данных через активное WebSocket-соединение."""
    global virtual_balance, portfolio

    print("[OK] Канал связи открыт. Подписка на Raydium...")

    subscribe_query = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [RAYDIUM_LIQUIDITY_POOL_V4]},
            {"commitment": "processed"}
        ]
    }
    await ws.send(json.dumps(subscribe_query))
    print(f"[OK] Подписка активна. Минимальный порог: {MIN_SWAP_AMOUNT_SOL} SOL")
    print(f"[OK] Порог ликвидности токена: ${MIN_LIQUIDITY_USD:,.2f}")
    if FULL_AUTO:
        print(f"[OK] Полный автомат включён. Тейк-профит: +{TAKE_PROFIT_PCT}%, Стоп-лосс: -{STOP_LOSS_PCT}%")
    elif ENABLE_SEMI_AUTO:
        print("[OK] Полуавтоматический режим покупки ВКЛЮЧЁН.")
    if RUGCHECK_ENABLED:
        print(f"[OK] Проверка Rugcheck включена. Кэш: {RUGCHECK_CACHE_MINUTES} мин.")
    print(f"[OK] Фильтр возраста пары: мин. {MIN_PAIR_AGE_MINUTES} мин, требовать DexScreener: {REQUIRE_DEXSCREENER_INFO}")
    print(f"[OK] Лимит открытых позиций: {MAX_OPEN_POSITIONS}, каскад: {CASCADE_TP_MINUTES}/{CASCADE_BE_MINUTES}/{HARD_TIMEOUT_MINUTES} мин")
    print("-" * 60)

    # Загрузка портфеля при старте мониторинга
    portfolio = load_portfolio()
    print(f"[OK] Портфель загружен: {len(portfolio)} позиций.")

    last_timeout_check = time.time()

    while True:
        message = await ws.recv()
        response = json.loads(message)

        # Периодическая проверка тайм-аута удержания и вывод баланса
        if time.time() - last_timeout_check >= 60:
            await check_hold_timeout(session)
            now = datetime.datetime.now().strftime('%H:%M:%S')
            print(f"\033[92m[{now}] [BALANCE] Текущий баланс: {virtual_balance:.4f} SOL\033[0m")
            last_timeout_check = time.time()

        if 'params' in response:
            value = response['params']['result']['value']
            signature = value.get('signature')
            logs = value.get('logs', [])

            if any("Instruction: Swap" in log for log in logs):
                tx_data = await fetch_tx_details(session, signature)
                if not tx_data or 'meta' not in tx_data:
                    continue

                # Проверка минимальной суммы свопа в SOL
                pre = tx_data['meta'].get('preBalances', [0])
                post = tx_data['meta'].get('postBalances', [0])
                change_sol = abs(post[0] - pre[0]) / 1e9

                if change_sol < MIN_SWAP_AMOUNT_SOL:
                    continue

                # Извлекаем адрес токена
                token_address = extract_token_from_tx(tx_data)
                if not token_address:
                    print(f"[WARNING] Не удалось извлечь адрес токена из транзакции {signature}")
                    continue

                # Получаем информацию о токене
                token_info = await fetch_token_info(session, token_address)
                if not token_info:
                    print(f"[WARNING] Нет данных о токене {token_address}, своп пропущен.")
                    continue

                liquidity = token_info['supply'] * token_info['price_per_token']

                # Проверка порога ликвидности
                if liquidity < MIN_LIQUIDITY_USD:
                    print(f"[WARNING] Своп {signature} пропущен: ликвидность ${liquidity:,.2f} < ${MIN_LIQUIDITY_USD:,.2f}")
                    continue

                # Проверка безопасности токена через Rugcheck (если включено)
                if RUGCHECK_ENABLED:
                    rug_status = await check_rugcheck(session, token_address)
                    if rug_status is False:
                        print(f"[RUGCHECK] Токен {token_info['symbol']} не прошёл проверку безопасности (rugpull/опасный). Своп пропущен.")
                        continue
                    elif rug_status is None:
                        print(f"[RUGCHECK] Не удалось проверить токен {token_info['symbol']}. Своп пропущен из предосторожности.")
                        continue

                # Фильтр возраста пары и объёма торгов (DexScreener)
                if MIN_PAIR_AGE_MINUTES > 0 or REQUIRE_DEXSCREENER_INFO or MIN_VOLUME_USD > 0:
                    pair_info = await fetch_pair_info(session, token_address)
                    # Проверка наличия данных DexScreener
                    if REQUIRE_DEXSCREENER_INFO and pair_info['age_minutes'] is None:
                        print(f"[DEX] Токен {token_info['symbol']} не найден в DexScreener. Своп пропущен.")
                        continue
                    # Проверка возраста пары
                    if pair_info['age_minutes'] is not None and pair_info['age_minutes'] < MIN_PAIR_AGE_MINUTES:
                        print(f"[AGE] Возраст пары {token_info['symbol']}: {pair_info['age_minutes']:.1f} мин (< {MIN_PAIR_AGE_MINUTES}). Своп пропущен.")
                        continue
                    # Проверка объёма торгов за 5 минут
                    if MIN_VOLUME_USD > 0:
                        volume_5m = pair_info.get('volume_5m')
                        if volume_5m is None:
                            print(f"[VOL] Нет данных об объёме для {token_info['symbol']}. Своп пропущен.")
                            continue
                        if volume_5m < MIN_VOLUME_USD:
                            print(f"[VOL] Объём {token_info['symbol']}: ${volume_5m:.2f} (< ${MIN_VOLUME_USD}). Своп пропущен.")
                            continue

                # Все проверки пройдены – выводим информацию о свопе
                print(f"\n[🔥 SWAP DETECTED]")
                print(f"TX: https://solscan.io/tx/{signature}")
                print(f"Сумма: {change_sol:.4f} SOL")
                print(f"Ликвидность: ${liquidity:,.2f}")
                print(f"Токен: {token_info['name']} ({token_info['symbol']})")
                if token_info['price_per_token']:
                    print(f"Цена токена: ${token_info['price_per_token']:.6f}")
                else:
                    print("Цена токена: Цена не определена")

                # Отправка уведомления в Telegram (не блокируем основной цикл)
                tg_message = (
                    f"🔥 SWAP {change_sol:.4f} SOL\n"
                    f"Ликвидность: ${liquidity:,.2f}\n"
                    f"Токен: {token_info['name']} ({token_info['symbol']})\n"
                    f"Ссылка: https://solscan.io/tx/{signature}"
                )
                asyncio.create_task(send_tg_notification(tg_message))
                # Информация о проскальзывании (заглушка, реальный контроль появится при интеграции Jupiter)
                if MAX_SLIPPAGE_PCT > 0:
                    pass  # место для будущей проверки через Jupiter Quote

                # Логика автоматической продажи (если токен уже в портфеле)
                if token_address in portfolio:
                    buy_price = portfolio[token_address]['buy_price']
                    current_price = token_info['price_per_token']
                    if current_price <= 0:
                        print("[WARNING] Не удалось определить текущую цену токена для оценки PnL.")
                        continue  # не продаём и не покупаем повторно

                    price_change_pct = (current_price - buy_price) / buy_price * 100

                    # Проверка условий тейк-профита или стоп-лосса
                    if price_change_pct >= TAKE_PROFIT_PCT or price_change_pct <= -STOP_LOSS_PCT:
                        amount = portfolio[token_address]['amount']
                        pnl_sol = amount * (price_change_pct / 100)
                        virtual_balance += amount + pnl_sol
                        save_balance(virtual_balance)

                        sim_trade = {
                            'timestamp': asyncio.get_event_loop().time(),
                            'token_address': token_address,
                            'symbol': token_info['symbol'],
                            'name': token_info['name'],
                            'sol_amount': amount,
                            'price_per_token': current_price,
                            'liquidity_usd': liquidity
                        }
                        await write_sim_trade(sim_trade, virtual_balance, side='SELL', pnl_sol=pnl_sol)

                        emoji = "✅" if pnl_sol >= 0 else "❌"
                        now = datetime.datetime.now().strftime('%H:%M:%S')
                        print(f"[{now}] >>> АВТО-ПРОДАЖА: {portfolio[token_address]['symbol']}, PnL: {pnl_sol:+.4f} SOL, Баланс: {virtual_balance:.4f} SOL {emoji}")
                        del portfolio[token_address]
                        save_portfolio(portfolio)  # сохраняем изменения
                        continue  # продажа выполнена, переходим к следующему свопу
                    else:
                        print(f"[INFO] Токен {token_info['symbol']} уже в портфеле, условия продажи не выполнены.")
                        continue  # позиция открыта, не покупаем

                # Если токена нет в портфеле – выполняем логику покупки
                if FULL_AUTO:
                    # Проверка лимита открытых позиций
                    if len(portfolio) >= MAX_OPEN_POSITIONS:
                        print(f"[INFO] Достигнут лимит открытых позиций ({MAX_OPEN_POSITIONS}). Своп пропущен.")
                        continue

                    # Проверка достаточности баланса
                    if virtual_balance < change_sol:
                        print(f"[WARNING] Недостаточно виртуального баланса ({virtual_balance:.4f} SOL). Покупка невозможна.")
                        continue
                    if round(change_sol, 6) > MAX_BUY_SOL:
                        print(f"[WARNING] Сумма свопа {change_sol:.4f} SOL превышает лимит {MAX_BUY_SOL} SOL. Покупка пропущена.")
                        continue
                    # Если боевой режим — выполняем реальный своп
                    if TRADING_MODE == "live":
                        success = await execute_jupiter_swap(session, token_address, token_info['symbol'], change_sol)
                        if success:
                            print(f">>> [LIVE] Реальная покупка: {token_info['symbol']} на {change_sol:.4f} SOL")
                            # Обновляем виртуальный баланс для логирования (зеркалируем реальную сделку)
                            virtual_balance -= change_sol
                            fee_sol = change_sol * (SIM_FEE_PCT / 100.0)
                            virtual_balance -= fee_sol
                            save_balance(virtual_balance)
                            portfolio[token_address] = {
                                'symbol': token_info['symbol'],
                                'name': token_info['name'],
                                'amount': change_sol,
                                'buy_price': token_info['price_per_token'],
                                'entry_time': time.time()
                            }
                            save_portfolio(portfolio)
                            # Запись сделки в CSV
                            sim_trade = {
                                'timestamp': asyncio.get_event_loop().time(),
                                'token_address': token_address,
                                'symbol': token_info['symbol'],
                                'name': token_info['name'],
                                'sol_amount': change_sol,
                                'price_per_token': token_info['price_per_token'],
                                'liquidity_usd': liquidity
                            }
                            await write_sim_trade(sim_trade, virtual_balance, side='BUY', pnl_sol=0.0)
                        else:
                            print(f"[LIVE] Реальная покупка {token_info['symbol']} не удалась.")
                        continue  # После реальной сделки переходим к следующему свопу

                    virtual_balance -= change_sol
                    # Списание комиссии симулятора
                    fee_sol = change_sol * (SIM_FEE_PCT / 100.0)
                    virtual_balance -= fee_sol
                    save_balance(virtual_balance)
                    print(f"[FEE] Комиссия: {fee_sol:.6f} SOL")

                    portfolio[token_address] = {
                        'symbol': token_info['symbol'],
                        'name': token_info['name'],
                        'amount': change_sol,
                        'buy_price': token_info['price_per_token'],
                        'entry_time': time.time()
                    }
                    save_portfolio(portfolio)  # сохраняем портфель после покупки

                    sim_trade = {
                        'timestamp': asyncio.get_event_loop().time(),
                        'token_address': token_address,
                        'symbol': token_info['symbol'],
                        'name': token_info['name'],
                        'sol_amount': change_sol,
                        'price_per_token': token_info['price_per_token'],
                        'liquidity_usd': liquidity
                    }
                    await write_sim_trade(sim_trade, virtual_balance, side='BUY', pnl_sol=0.0)

                    now = datetime.datetime.now().strftime('%H:%M:%S')
                    print(f"[{now}] >>> АВТО-ПОКУПКА: {token_info['symbol']} на {change_sol:.4f} SOL")
                    print(f"Виртуальный баланс после сделки: {virtual_balance:.4f} SOL")

                elif ENABLE_SEMI_AUTO:
                    # Проверка лимита открытых позиций
                    if len(portfolio) >= MAX_OPEN_POSITIONS:
                        print(f"[INFO] Достигнут лимит открытых позиций ({MAX_OPEN_POSITIONS}). Своп пропущен.")
                        continue

                    # Полуавтоматический режим с подтверждением
                    confirm = await ask_confirmation()
                    if confirm is True:
                        if virtual_balance < change_sol:
                            print(f"[WARNING] Недостаточно виртуального баланса ({virtual_balance:.4f} SOL). Сделка не записана.")
                        else:
                            if round(change_sol, 6) > MAX_BUY_SOL:
                                print(f"[WARNING] Сумма свопа {change_sol:.4f} SOL превышает лимит {MAX_BUY_SOL} SOL. Покупка пропущена.")
                                continue
                            virtual_balance -= change_sol
                            # Списание комиссии симулятора
                            fee_sol = change_sol * (SIM_FEE_PCT / 100.0)
                            virtual_balance -= fee_sol
                            save_balance(virtual_balance)
                            print(f"[FEE] Комиссия: {fee_sol:.6f} SOL")

                            portfolio[token_address] = {
                                'symbol': token_info['symbol'],
                                'name': token_info['name'],
                                'amount': change_sol,
                                'buy_price': token_info['price_per_token'],
                                'entry_time': time.time()
                            }
                            save_portfolio(portfolio)  # сохраняем портфель после покупки

                            sim_trade = {
                                'timestamp': asyncio.get_event_loop().time(),
                                'token_address': token_address,
                                'symbol': token_info['symbol'],
                                'name': token_info['name'],
                                'sol_amount': change_sol,
                                'price_per_token': token_info['price_per_token'],
                                'liquidity_usd': liquidity
                            }
                            await write_sim_trade(sim_trade, virtual_balance, side='BUY', pnl_sol=0.0)
                            print(f"Сделка записана в симулятор: {token_info['symbol']} на {change_sol:.4f} SOL")
                            print(f"Виртуальный баланс после сделки: {virtual_balance:.4f} SOL")
                    elif confirm is False:
                        print("Пропущено.")
                    else:
                        print("Пропущено (нет ответа).")

        elif 'result' in response:
            print(f"[SYSTEM] Subscription ID: {response['result']}")


async def connect_with_retry(session):
    """Управление подключением с экспоненциальной задержкой."""
    backoff = 1
    max_backoff = 60

    while True:
        try:
            print(f"[{'='*10}] Попытка подключения к Helius RPC...")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                print("[OK] Подключено успешно.")
                await monitor(ws, session)
        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError,
                OSError, asyncio.TimeoutError, Exception) as e:
            print(f"\n[CRITICAL ERROR] Соединение разорвано: {e}")
            print(f"Повтор через {backoff} секунд...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

def print_dashboard():
    """Выводит сводку по итогам симуляции при остановке."""
    try:
        with open(SIM_TRADES_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print("\n[!] Файл сделок не найден, дашборд недоступен.")
        return

    if not rows:
        print("\n[!] Нет данных для дашборда.")
        return

    buys = [r for r in rows if r['side'] == 'BUY']
    sells = [r for r in rows if r['side'] == 'SELL']
    sell_pnls = [float(r['pnl_sol']) for r in sells]
    total_pnl = sum(sell_pnls)
    best = max(sell_pnls, default=0)
    worst = min(sell_pnls, default=0)
    profit_trades = [p for p in sell_pnls if p > 0]
    loss_trades = [p for p in sell_pnls if p < 0]

    print("\n" + "=" * 50)
    print("   ИТОГОВЫЙ ДАШБОРД СЕССИИ")
    print("=" * 50)
    print(f"Всего сделок BUY (покупок):     {len(buys)}")
    print(f"Всего сделок SELL (продаж):     {len(sells)}")
    print(f"Открытых позиций:               {len(buys) - len(sells)}")
    print(f"Общий PnL (прибыль/убыток):     {total_pnl:+.6f} SOL")
    if sells:
        print(f"Прибыльных сделок (profit):      {len(profit_trades)} ({len(profit_trades)/len(sells)*100:.1f}%)")
        print(f"Убыточных сделок (loss):         {len(loss_trades)} ({len(loss_trades)/len(sells)*100:.1f}%)")
        print(f"Лучшая сделка (best):            {best:+.6f} SOL")
        print(f"Худшая сделка (worst):           {worst:+.6f} SOL")
    if rows:
        last = rows[-1]
        print(f"Текущий баланс:                  {float(last['balance_after']):.4f} SOL")
    print("=" * 50)
async def main():
    """Точка входа."""
    global portfolio

    # Инициализация CSV-файла симулятора при необходимости
    if (FULL_AUTO or ENABLE_SEMI_AUTO) and not os.path.exists(SIM_TRADES_FILE):
        with open(SIM_TRADES_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'token_address', 'symbol', 'name', 'side',
                             'sol_amount', 'price_per_token', 'liquidity_usd',
                             'balance_after', 'pnl_sol'])
        print(f"[OK] Создан файл симулятора сделок: {SIM_TRADES_FILE}")

    # Очистка портфеля, если оба торговых режима отключены
    if not FULL_AUTO and not ENABLE_SEMI_AUTO:
        save_portfolio({})

    # Загрузка портфеля для отображения стартовой информации
    portfolio = load_portfolio()
    print(f"[OK] Портфель загружен: {len(portfolio)} позиций.")

    print("=" * 60)
    print("   SOLANA REAL-TIME RADAR: AUTO-RECONNECT + FILTER ACTIVE   ")
    print(f"   Порог суммы: {MIN_SWAP_AMOUNT_SOL} SOL, макс. вход: {MAX_BUY_SOL} SOL")
    print(f"   Порог ликвидности: ${MIN_LIQUIDITY_USD:,.2f}")
    print(f"   Стартовый виртуальный баланс: {virtual_balance:.4f} SOL")
    if FULL_AUTO:
        print(f"   Тейк-профит: +{TAKE_PROFIT_PCT}%, Стоп-лосс: -{STOP_LOSS_PCT}%")
    if RUGCHECK_ENABLED:
        print(f"   Проверка Rugcheck: ВКЛ (кэш {RUGCHECK_CACHE_MINUTES} мин)")
    print(f"   Фильтр возраста пары: мин. {MIN_PAIR_AGE_MINUTES} мин, требовать DexScreener: {REQUIRE_DEXSCREENER_INFO}")
    print(f"   Фильтр объёма торгов: мин. ${MIN_VOLUME_USD} (5 мин)")
    print(f"   Лимит открытых позиций: {MAX_OPEN_POSITIONS}, каскад: {CASCADE_TP_MINUTES}/{CASCADE_BE_MINUTES}/{HARD_TIMEOUT_MINUTES} мин")
    print("=" * 60)

    # Автоматический запрос 2 SOL в Devnet через Helius
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "requestAirdrop",
            "params": ["2ZPwwWt85geEwjdQtpzApZN6P6rzysVXRYFnvoMeBKN2", 2000000000]
        }
        async with aiohttp.ClientSession() as tmp_session:
            async with tmp_session.post(HTTP_URL, json=payload) as resp:
                if resp.status == 200:
                    print("[OK] Запрос на пополнение Devnet кошелька отправлен!")
                else:
                    print("[INFO] Кран через API ответил статусом:", resp.status)
    except Exception as e:
        print("[WARNING] Не удалось запросить монеты автоматом:", e)
    async with aiohttp.ClientSession() as session:
        await connect_with_retry(session)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Система остановлена оператором.")
        print_dashboard()
