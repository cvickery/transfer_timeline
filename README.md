# Use CUNYfirst data to track how courses transfer across CUNY.

This project extracts information about how and when courses transfer between CUNY colleges.
There are two parts: building a local database, and generating reports based on that database.

## Build Local Database

All data comes from CUNYfirst, the result of running queries manually or on schedule. In particular,
information about when and how transfer credits are evaluated is updated daily. Each time a
student’s courses are evaluated, the date of any previous evaluation is overwritten, but we keep
track of each evaluation by running a query daily and recording diffs.

The following information is kept in the local database:

- Sessions: (Start Registration, Classes Start) come from SESSION\_TBL, which gives this information
  for each college for each term.
- Admissions: (Apply, Admit, Commit, Matric), ADM\_MC\_VW, gives this information for
  each student for each term.
- Transfers Applied: (First Eval, Latest Eval), a query links several tables together:
    - TRNS\_CRSE\_SCH gives the sending college and receiving college for each student.
    - TRNS\_CRSE\_TERM gives the posted date for each evaluation for each articulation term.
    - Other tables give information about the courses that are being transferred.
- Registrations (First Registration, Latest Registration), a view, CU\_STD\_ENRL\_VW, gives
  the class that each student registers for, for each term.

## Reports

For the time-to-evaluate project, there are ten event dates: Apply, Admit, Commit, Matric, First
Eval, Latest Eval, Start Registration (for a college), First Registration (by a student), Latest
Registration (by a student), and Classes Start. A cohort consists of all students who apply to
transfer to a given college in a particular term. For a given pair of the 45 possible event date
pairs, generate a frequency distribution of complete pairs (i.e. where both events exist for a
student), and produce descriptive statistics for the intervals between those events.
