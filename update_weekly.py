#! /usr/local/bin/python3
""" Archive current set of admissions/registration/sessions tables and replace them with current
    ones from Tumbleweed.

    This should be scheduled to run once a week, after the following queries have been run and sent
    to Tumbleweed:
      ADMIT_ACTION_TBL
      ADMIT_TYPE_TBL
      CV_QNS_ADMISSIONS
      CV_QNS_STUDENT_SUMMARY
      PROG_REASON_TBL
      QNS_CV_SESSION_TABLE
"""

import os
import re
import socket
import sys

from argparse import ArgumentParser
from datetime import date
from pathlib import Path
from subprocess import run
from thin import thin

if __name__ == '__main__':

  arg_parser = ArgumentParser('Update CF Query files')
  arg_parser.add_argument('--skip_downloads', '-sd', action='store_true')
  args = arg_parser.parse_args()

  # Must be in the transfer_timeline project dir
  home_dir = Path.home()
  project_dir = Path(home_dir, 'Projects/transfer_timeline')
  os.chdir(project_dir)

  archive_dir = Path('./queries_archive')
  queries_dir = Path('./queries')
  query_downloads_dir = Path('./query_downloads')

  print('Download Queries')
  run(['lftp', '-f', 'getcunyrc'])
  exit('Still under development')
  # Archive current versions, with file date added
  admit_reg_files = Path(queries_dir).glob('*')
  for admit_reg_file in admit_reg_files:
    file_name = admit_reg_file.name
    if file_name.startswith('.'):
      # Ignore hidden files
      continue
    file_date = date.fromtimestamp(admit_reg_file.stat().st_ctime)
    base_name = admit_reg_file.stem
    # Current query files don’t have numbers in them (CF job-id or date)
    if not re.search(r'\d+', base_name):
      new_name = f'{base_name}_{file_date}.csv'
      new_file = Path(archive_dir, new_name)
      new_file.write_bytes(admit_reg_file.read_bytes())
      print(f'Copied {file_name} to {archive_dir.name}/{new_name}')
    else:
      print(f'Stray file: {queries_dir.name}/{admit_reg_file.name}')

  if not args.skip_downloads:
    # Download and rename new versions, if possible
    hostname = socket.gethostname()
    if not hostname.lower().endswith('.cuny.edu'):
      # Try downloading from Babbage
      print(f'Unable to access Tumbleweed from {hostname}; trying Babbage')
      trex_labs = f'149.4.44.244:Projects/transfer_timeline/queries/*'
      completed_process = run(['/usr/bin/scp', trex_labs, '.'])
      if completed_process.returncode != 0:
        print('Download from Babbage FAILED', file=sys.stderr)

    else:
      print('Download from Tumbleweed')
      completed_process = run(['/usr/local/bin/lftp', '-f', './getcunyrc'],
                              stdout=sys.stdout, stderr=sys.stdout)
      if completed_process.returncode != 0:
        print('Download from Tumbleweed FAILED', file=sys.stderr)

      for admit_reg_file in queries_dir.glob('*'):
        # New files from Tumbleweed have job-id numbers: rename them to just the base_name, thereby
        # overwriting the ones that were archived above. Match job numbers (all digits, no hyphens), not
        # dates (digits and hyphens). (During development there were dated files in queries_dir.)
        if match := re.search(r'(^.*)-\d+.csv', admit_reg_file.name):
          new_name = f'{match[1]}.csv'
          admit_reg_file.rename(f'{queries_dir}/{new_name}')
          print(f'  Renamed:        {admit_reg_file.name} to {new_name}')
        else:
          print(f'Unchanged: {admit_reg_file.name}')

  # Prune the Archive directory
  max_archive_size_units = 'GB'
  num_archive_size_units = 20
  max_archive_size = num_archive_size_units * pow(2, 30)
  print(f'Pruning {archive_dir} to {num_archive_size_units:,}{max_archive_size_units}')
  thin(str(archive_dir), max_archive_size, only_suffixes=['csv'])
