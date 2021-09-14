import tinvest
import json
import os
import asyncio
import aiopg

TOKEN = os.getenv('TINVEST_TOKEN')


async def get_and_insert_stocks():
    conn = await aiopg.connect(database='celery_ti', user='celery_user',
                               password='pass', host='127.0.0.1', port=5432)
    cur = await conn.cursor()
    await cur.execute("BEGIN;")
    await cur.execute(
        "CREATE TABLE IF NOT EXISTS stocks (id serial primary key, ticker varchar, figi varchar, name varchar, currency varchar, price numeric);")
    await cur.execute("COMMIT;")

    client = tinvest.AsyncClient(TOKEN)
    response = await client.get_market_stocks()
    stocks = json.loads(response.json())["payload"]["instruments"]

    for i in range(10):
        stock = stocks[i]
        await cur.execute("BEGIN;")
        await cur.execute(
            "INSERT INTO stocks (ticker, figi, name, currency, price) VALUES ((%s), (%s), (%s), (%s), (%s))",
            (stock["ticker"], stock["figi"], stock["name"], stock["currency"], 0))
        await cur.execute("COMMIT;")

    await client.close()
    await conn.close()


asyncio.run(get_and_insert_stocks())
