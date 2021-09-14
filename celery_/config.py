import asyncio

import tinvest
import json
import os
import aiopg

from celery import Celery

app = Celery('config', broker='redis://127.0.0.1:6379/0')

TOKEN = os.getenv('TINVEST_TOKEN')


@app.task
def retrieve_stocks():
    async def async_retrieve():
        client = tinvest.AsyncClient(TOKEN)
        conn = await aiopg.connect(database='celery_ti', user='celery_user',
                                   password='pass', host='127.0.0.1')
        cur = await conn.cursor()
        await cur.execute("SELECT * FROM stocks;")
        stocks = await cur.fetchall()  # (id, ticker, figi, name, currency, price)

        for stock in stocks:
            stock_figi = stock[2]
            response = await client.get_market_orderbook(stock_figi, 20)

            last_price = json.loads(response.json())["payload"]["last_price"]
            await cur.execute("BEGIN;")
            await cur.execute("UPDATE stocks SET price = (%s) WHERE figi = (%s);", (last_price, stock_figi))
            await cur.execute("COMMIT;")

        await client.close()
        await conn.close()

    asyncio.run(async_retrieve())


app.add_periodic_task(10.0, retrieve_stocks.s(), name='retrieve-stocks')
