#! /usr/local/bin/python3
""" Archive current set of admissions/registration/sessions tables and replace them with current
ones from Tumbleweed.
"""

import socket
import sys

from datetime import date
from pathlib import Path

if __name__ == '__main__':
  hostname = socket.gethostname()
  if not hostname.lower().endswith('.cuny.edu'):
    print(f'Unable to access Tumbleweed from {hostname}')

  archive_dir = Path('./Admissions_Registrations_Archive')
  admit_reg_dir = Path('Admissions_Registrations')
  admit_reg_files = Path(admit_reg_dir).glob('*')
  for admit_reg_file in admit_reg_files:
    file_name = admit_reg_file.name
    if file_name.startswith('.'):
      continue
    file_date = date.fromtimestamp(admit_reg_file.stat().st_ctime)
    base_name = file_name[0:file_name.index('-')]
    new_name = f'{base_name}-{file_date}.csv'
    new_file = Path(archive_dir, new_name)
    new_file.write_bytes(admit_reg_file.read_bytes())
    print(f'Copied {new_file}')
