import csv
import sys

def analyze(csv_file='sim_trades.csv'):
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Файл {csv_file} не найден.")
        return

    if not rows:
        print("Нет данных для анализа.")
        return

    # Подсчёт
    buys = [r for r in rows if r['side'] == 'BUY']
    sells = [r for r in rows if r['side'] == 'SELL']
    sell_pnls = [float(r['pnl_sol']) for r in sells]

    total_pnl = sum(sell_pnls)
    profit_trades = [p for p in sell_pnls if p > 0]
    loss_trades = [p for p in sell_pnls if p < 0]
    best = max(sell_pnls, default=0)
    worst = min(sell_pnls, default=0)

    # Вывод
    print("=" * 50)
    print("   СТАТИСТИКА СИМУЛЯТОРА")
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
        print(f"Текущий баланс:        {float(last['balance_after']):.4f} SOL")

if __name__ == '__main__':
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'sim_trades.csv'
    analyze(csv_file)
