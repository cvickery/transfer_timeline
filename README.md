# Tracking the Transfer Admissions Process across CUNY.

This project extracts information about the sequence of events involved when a student transfers
from one CUNY college to another, with particular attention to transfer credit evaluation.

## Timeline Events

For the time-to-evaluate project, we look at ten event dates that mark a candidate’s progress through the transfer admissions process: application, admission, transfer credit evaluation, etc. See table below. A cohort consists of all students who apply to transfer from one CUNY Collegee to another CUNY college in a particular term (*admit term*). For a given pair of the 45 possible event date pairs, generate a frequency distribution of complete pairs (i.e. where both events exist for a student), and produce descriptive statistics for the intervals between those events.

The intervals reported by default are defined by the following pairs of events:

| Start | End | Description |
| :---- | :-- | :---------- |
| apply | admit | Time it takes the college to respond to an application |
| admit | commit | Time it takes an admitted student to commit to attending |
| commit | matric | Time it takes the college to matriculate a committed student |
| admit | matric | Total time from admission to matriculation |
| admit | first eval | Time it takes the college to evaluate a student’s transfer credits after admission^1 |
| admit | latest eval | Time from admission to the latest of any credit re-evaluations
| admit | start open enr | How long after a student applied was it possible for _any_ student to apply |
| commit | first eval | How long after a student committed until their courses were evaluated^1 |
| commit | latest eval | How long after a student committed until their latest credit evaluation evaluatedv
| matric | first eval | How long after a student matriculated until their coursese were evaluated^1 |
| matric | latest eval | How long after a student matriculated until their latest credit re-evaluation |
| first eval | start open enr | How long after credits were evaluated before registration period started |
| latest eval | start open enr | How long after most-recent credit evaluated before registration period started |
| first eval | start classes | Lead time between first credit evaluation and start of classes |
| latest eval | start classes | Lead time between latest credit evaluation and start of classes |
| first eval | census date | Lead time between first credit evaluation and end of free add/drop period |
| latest eval | census date | Lead time between latest credit evaluation and end of free add/drop period |

1 Should be negative!

## Implementation
There are two parts: building a local database, then generating reports based on that database.

## Build Local Database

All data comes from CUNYfirst, the result of running queries manually or on schedule. In particular, information about when and how transfer credits are evaluated is updated daily. Each time a student’s courses are evaluated, the date of any previous evaluation is overwritten, but we keep track of each evaluation by running a query daily and recording differences.

The following information is kept in the local database (names in caps are CUNYfirst query names):

- *Sessions*: (Start Registration, Classes Start) come from SESSION\_TBL, which gives this information
  for each college for each term.
- *Admissions*: (Apply, Admit, Commit, Matric), ADM\_MC\_VW, gives this information for
  each student for each term.
- *Transfers Applied*: (First Eval, Latest Eval), a query links several tables together:
    - TRNS\_CRSE\_SCH gives the sending college and receiving college for each student.
    - TRNS\_CRSE\_TERM gives the posted date for each evaluation for each articulation term.
    - Other tables give information about the courses that are being transferred.
- *Registrations* (First Registration, Latest Registration), a view, CU\_STD\_ENRL\_VW, gives
  the class that each student registers for, for each term.

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

### Cohorts

A _cohort_ is a set of students who apply for transfer admission to a college in a given semester, known as the *articulation term*. Not all students who apply go through all the possible steps in the timeline from application to registration for courses at the destination college, so the size of the cohort can vary from step to step in the process. Examples are a student who applies but is not admitted, or who is admitted but doesn't register. But there are cases where a student is simply missing a step in the process; an example being a student who does not make a deposit after being admitted because of an administrative policy that makes this step optional.

### Admit Term

Students are admitted for either the Fall or Spring term, but Fall admits are typically allowed to register for courses during the summer prior to their admit term. This can skew the data for intervals that end with the dates when the student registered or started classes.

## Report Generation

The script _generate\_timeline\_stats.sh_ checks that the timeline table queries are up to date,
and then uses _generate\_timeline\_stats.py_ to generate statistical reports on the number of days
between pairs of events. A master Excel spreadsheet is saved in the project directory, Markdown
reports for each cohort and measure are saved in the _reports_ directory, and detailed timeline CSV
files are saved in the _timelines_ directory.

The _generate\_timeline\_stats.sh_ can be edited to select the cohorts and date-pairs to be
reported.
