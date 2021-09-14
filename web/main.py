import aiopg
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/")
async def main():
    conn = await aiopg.connect(database='celery_ti', user='celery_user',
                               password='pass', host='127.0.0.1')
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM stocks;")
    stocks = await cur.fetchall()

    await conn.close()

    response = {}
    # (id, ticker, figi, name, currency, price)
    for _, ticker, _, name, currency, price in stocks:
        response[f"{name} + [{ticker}]"] = f"{price} {currency}"

    return JSONResponse(content=jsonable_encoder(response))
