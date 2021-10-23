#! /usr/local/bin/python3

def min_sec(arg: float) -> str:
  mins, secs = divmod(arg, 60)
  return f'{int(mins)}:{int(secs):02}'
