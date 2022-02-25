# Use CUNYfirst data to track how courses transfer across CUNY.

This project extracts information about the sequence of events involved when a student transfers
from one CUNY college to another, with particular attention to transfer credit evaluation.

There are two parts: building a local database, then generating reports based on that database.

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

## Procedures

### Initial setup

The CUNYfirst query CV\_QNS\_TRNS\_DTL\_SRC\_CLASS\_FULL provides information about transfer
evaluation events from Summer 2019 to date. _populate\_transfers\_applied.py_ uses that query to
create and populate the transfers\_applied table in the cuny\_transfers database. Running the query,
downloading the result to this project’s _downloads_ directory, and running
_populate\_transfers\_applied.py_ are all done manually.

### Automatic Updates

When a student’s credits are re-evaluated, CUNYfirst overwrites the previous evaluation and does not
keep a record of the changes. We want that history information, so the query
CV\_QNS\_TRNS\_DTL\_SRC\_CLASS\_ALL is scheduled to run daily on CUNYfirst. A daily _cron_ job on
babbage.cs.qc.cuny.edu gets that query result from Tumbleweed and moves it into the _downloads_
directory of this project. Another daily _cron_ job then runs _update\_transfers\_applied.py_ to add
new (re-)evaluations to the _transfers\_applied_ table.

### Managing Other Tables

Queries for Admissions (_CV\_QNS\_ADMISSIONS_), Registrations(_CV\_QNS\_STUDENT\_SUMMARY_), and
Sessions (_QNS\_CV\_SESSION\_TABLE_) provide the dates for the events other than credit evaluations.
These queries, along with some secondary tables giving details about fields in the Admissions
table, are run manually on CUNYfirst, and saved in the _Admissions\_Registrations_ directory. The
_build\_timeline\_tables.py_ module uses those queries to create and populate the remaining tables
in the database.

### Grouped Timelines

The script _grouped\_timelines.py_ generates CSV files for student cohorts (College, Term) in a
format intended to facilitate spot-checking the event data available in the database. It is run
manually.

### Report Generation

The script _generate\_timeline\_stats.sh_ checks that the timeline table queries are up to date,
and then uses _generate\_timeline\_stats.py_ to generate statistical reports on the number of days
between pairs of events. A master Excel spreadsheet is saved in the project directory, Markdown
reports for each cohort and measure are saved in the _reports_ directory, and detailed timeline CSV
files are saved in the _timelines_ directory.

The _generate\_timeline\_stats.sh_ can be edited to select the cohorts and date-pairs to be
reported.
