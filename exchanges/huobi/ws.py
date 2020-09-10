import asyncio
import websockets
import gzip
from typing import Optional
from websockets.client import WebSocketClientProtocol
import ujson
import random
from asyncio.queues import Queue
from utils.asyncio import async_run, async_task
from datetime import datetime


class HuobiWebsocket:

  def __init__(self, notify_queue: Queue = None):
    self._ws: Optional[WebSocketClientProtocol] = None
    self._stop = False
    self._subscribes = Queue(maxsize=16)
    self._notify_queue = notify_queue
    self._last_ts = datetime.now().timestamp()

  async def _connect(self):
    async with websockets.connect('wss://api-aws.huobi.pro/ws') as ws:
      self._ws = ws
      while 1:
        if self._stop or not self._ws:
          break
        origin_bytes = await ws.recv()
        data = gzip.decompress(origin_bytes).decode('utf-8')
        await self.process_data(ujson.loads(data))
    self._ws = None

  async def _subscribe_loop(self):
    while 1:
      if self._stop:
        break
      if not self._ws:
        await asyncio.sleep(1)
        continue
      symbol = await self._subscribes.get()
      await self._subscribe_kline(symbol, '1min')

  async def _online_watching(self):
    while 1:
      await asyncio.sleep(60)  # 60秒检测
      now_ts = datetime.now().timestamp()
      if now_ts - self._last_ts >= 120:  # 2分钟超时
        break
    self.reconnect()

  async def _close(self):
    await self._ws.close()

  async def send(self, data):
    await self._ws.send(ujson.dumps(data))

  async def process_data(self, data):
    if 'ping' in data:
      await self.send({'pong': data['ping']})
      return
    self._last_ts = datetime.now().timestamp()
    if 'status' in data:
      return
    if 'ch' in data and 'tick' in data:
      await self.process_tick(data)
      return

    # todo more type

  async def process_tick(self, data):
    ch, ts, tick = data['ch'], data['ts'], data['tick']
    assert isinstance(ch, str)
    if ch.startswith('market.'):
      _, symbol, sub_type, period = ch.split('.')
      if sub_type == 'kline':
        if self._notify_queue:
          await self._notify_queue.put((symbol, ts, tick))
        return

      # todo more type

  def connect(self):
    self._stop = False
    async_task(self._subscribe_loop())
    async_task(self._connect())
    async_task(self._online_watching())

  def reconnect(self):
    self._stop = True
    self._subscribes = []
    async_run(self._close())
    self.connect()

  def subscribe(self, symbol):
    async_run(self._subscribes.put(symbol))

  async def _subscribe_kline(self, symbol: str, period: str):
    await self.send({'sub': f'market.{symbol.lower()}.kline.{period}', 'id': str(random.randint(0, 1000))})


def run():
  huobi = HuobiWebsocket()
  huobi.connect()
  huobi.subscribe('btcusdt')
  asyncio.get_event_loop().run_forever()
