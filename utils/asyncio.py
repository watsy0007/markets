import asyncio


def async_run(func):
  asyncio.get_event_loop().run_until_complete(func)


def async_task(func):
  asyncio.get_event_loop().create_task(func)
