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

import re
import socket
import sys

from datetime import date
from pathlib import Path
from subprocess import run

if __name__ == '__main__':

  # Must be in the transfer_timeline dir
  cwd = str(Path.cwd())
  archive_dir = Path('./Admissions_Registrations_Archive')
  admit_reg_dir = Path('./Admissions_Registrations')
  assert admit_reg_dir.is_dir(), f'No Admissions_Registrations dir'
  assert archive_dir.is_dir(), f'No Admissions_Registrations_Archive dir'

  # Archive current versions, with file date added
  admit_reg_files = Path(admit_reg_dir).glob('*')
  for admit_reg_file in admit_reg_files:
    file_name = admit_reg_file.name
    if file_name.startswith('.'):
      # Ignore hidden files
      continue
    file_date = date.fromtimestamp(admit_reg_file.stat().st_ctime)
    base_name = admit_reg_file.stem
    if str(file_date) not in base_name:
      new_name = f'{base_name}_{file_date}.csv'
      new_file = Path(archive_dir, new_name)
      new_file.write_bytes(admit_reg_file.read_bytes())
      print(f'Copied {file_name} to {archive_dir.name}/{new_name}')

  # Download and rename new versions, if possible
  hostname = socket.gethostname()
  if not hostname.lower().endswith('.cuny.edu'):
    print(f'Unable to access Tumbleweed from {hostname}; trying T-Rex Labs')
    # Try downloading from T-Rex Labs
    trex_labs = f'149.4.44.244:{cwd}/Admissions_Registrations/*'
    completed_process = run(['/usr/bin/scp', trex_labs, '.'])
    if completed_process.returncode != 0:
      exit('Download from T-Rex Labs FAILED')

  else:
    print('Download from Tumbleweed')
    completed_process = run(['/usr/local/bin/lftp', '-f', './getcunyrc'],
                            stdout=sys.stdout, stderr=sys.stdout)
    if completed_process.returncode != 0:
      exit('Download from Tumbleweed FAILED')

  for admit_reg_file in admit_reg_dir.glob('*'):
    # New files from Tumbleweed have job-id numbers: rename them to just the base_name, thereby
    # overwriting the ones that were archived above. Match job numbers (all digits, no hyphens), not
    # dates (digits and hyphens).
    if match := re.search(r'(^.*)-\d+.csv', admit_reg_file.name):
      new_name = f'{match[1]}.csv'
      admit_reg_file.rename(f'{admit_reg_dir}/{new_name}')
      print(f'Renamed {admit_reg_file.name} to {new_name}')

