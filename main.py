import typer
import asyncio
from typing import Tuple
from itertools import zip_longest

app = typer.Typer()


@app.command()
def huobi_fluctuation(params: Tuple[str, int, float] = typer.Option((str, int, float))):
  """
  火币行情波动
  params = btcusdt,eth, 30 0.01
  symbols 订阅符号数组 btcusdt ethusdt
  interval 时间间隔（秒）
  threshold 阈值（0.05 == 5%)
  """
  from exchanges.huobi.ws import HuobiWebsocket
  from exchanges.fluctuation import Fluctuation
  symbols, interval, threshold = params
  if not symbols:
    typer.echo("请配置symbols")
    exit()
  symbols = symbols.split(',')
  fluctuation = Fluctuation(dict(zip_longest(symbols, [None])), interval, threshold)
  fluctuation.watching()
  huobi = HuobiWebsocket(fluctuation.queue)
  huobi.connect()
  [huobi.subscribe(sym) for sym in symbols]

  asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
  app()
