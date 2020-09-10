from dataclasses import dataclass
from asyncio.queues import Queue
from typing import Dict, List, Set, Optional
from utils.asyncio import async_task


@dataclass
class SymbolThreshold:
  symbol: str
  ticks: Optional[List[Dict]] = None  # 时间 -> k线
  interval: int = 5
  threshold: float = 0.001
  trigger_ts: int = 0
  o: float = 0.0

  def append(self, tick: dict):

    o, ts = tick['open'], tick['id']
    if not self.ticks:
      self.append_tick(tick)
    # 达到阈值上线, 移除最老的一根， 并更新相关数值
    if ts - self.ticks[0]['id'] > self.interval:
      self.ticks = self.ticks[1:]

    if self.ticks[-1]['id'] != ts:
      self.append_tick(tick)
      return
    self.ticks[-1] = tick

  def is_trigger(self) -> (bool, bool, float):
    tick = self.ticks[-1]
    c, ts = tick['close'], tick['id']
    if ts == self.trigger_ts:
      return False, False, 0.0
    change = (c - self.o) / self.o
    if abs(change) > self.threshold:
      self.trigger_ts = ts
      return True, c > self.o, change
    return False, False, 0.0

  def append_tick(self, tick):
    self.ticks.append(tick)
    self.o = self.ticks[0]['open']


@dataclass
class Fluctuation:
  symbols: Dict[str, Optional[SymbolThreshold]] = None
  interval: int = 60 * 5  # 5分钟间隔
  threshold: float = 0.05
  queue: Queue = Queue(maxsize=16)
  stop: bool = False

  def watching(self):
    async_task(self._watching())

  async def _watching(self):
    while 1:
      if self.stop:
        break
      sym, ts, tick = await self.queue.get()
      if sym not in self.symbols:
        continue
      await self._check_threshold(sym, ts, tick)

  async def _check_threshold(self, symbol, ts, tick):
    if not self.symbols[symbol]:
      self.symbols[symbol] = SymbolThreshold(symbol, [], self.interval, self.threshold)
    threshold = self.symbols.get(symbol)
    threshold.append(tick)
    trigger, arrow, change = threshold.is_trigger()
    if trigger:
      print(trigger, arrow, change, threshold)
