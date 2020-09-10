import inspect


def loop(f):
  def wrapper(*args, **kwargs):
    if inspect.isfunction(f):
      return f(*args, **kwargs)

  return wrapper
