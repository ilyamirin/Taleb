import csv
import numpy as np
from pymongo import MongoClient
import datetime

generation = datetime.datetime.utcnow()

prices = {}
with open('usd_quotes.csv', newline='') as csvfile:
    rdr = csv.reader(csvfile, delimiter=';', quotechar='|')
    for row in rdr:
        prices[row[0]] = float(row[1])

traders = np.arange(2 * 10000).reshape(10000, 2)
for i in range(10000):
    traders[i - 1] = [10000, 1000]


def in_rubles(trader_to_count, rate):
    return int(trader_to_count[0] + trader_to_count[1] * rate)


def calc_totals(traders_to_count, rate):
    total = []
    for trd in traders_to_count:
        total.append(in_rubles(trd, rate))
    total = sorted(total)
    res = {'currency_rate': rate,
           'traders': len(total),
           'gen': generation,
           'richest': total[-1], 'poorest': total[0],
           'welfare': int(np.sum(total)), 'mean': int(np.mean(total)),
           'upper10': int(np.sum(total[-10:])), 'lower10': int(np.sum(total[:10]))}
    return res


results = []

client = MongoClient()
db = client.Taleb
totals_collection = db.totals
traders_collection = db.traders

for date in reversed(list(prices.keys())):
    currency_rate = prices[date]

    next_traders_generation = []

    for trader in traders:
        if in_rubles(trader, currency_rate) < (100 * currency_rate):
            continue

        traders_collection.insert_one({'trader': [int(trader[0]), int(trader[1])], 'gen': generation, 'rate': currency_rate})

        bid = 0
        if np.random.rand() > 0.5:
            bid = np.random.randint(trader[0])
            trader[0] -= bid
            trader[1] += bid / currency_rate
        else:
            bid = np.random.randint(trader[1])
            trader[1] -= bid
            trader[0] += bid * currency_rate

        next_traders_generation.append(trader)

    traders = next_traders_generation

    totals = calc_totals(traders, currency_rate)
    totals['date'] = date
    results.append(totals)
    totals_collection.insert_one(totals)
# for date
