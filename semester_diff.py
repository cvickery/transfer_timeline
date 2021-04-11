#! /usr/local/bin/python3
""" Utility function to tell the number of Fall/Spring semesters between one and another, using
    CUNYfirst term codes.
"""
import sys


# semester_diff()
# -------------------------------------------------------------------------------------------------
def semester_diff(first_semester: int, second_semester: int) -> int:
  """ Return the number of semesters between the first and second. Will be negative if first
      comes after second.
      Based on CUNY term codes, C-YY-M, which are assumed to be spring (M = 2) or fall (M = 9).
  """
  years_1, months_1 = divmod(second_semester, 10)
  years_2, months_2 = divmod(first_semester, 10)
  return 2 * (years_2 - years_1) + round(((1 + months_2 % 10) - (1 + months_1 % 10)) / 5)


if __name__ == '__main__':
  print(semester_diff(int(sys.argv[1]), int(sys.argv[2])))
