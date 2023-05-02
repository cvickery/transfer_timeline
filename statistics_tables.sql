-- Create tables for timeline statistics and latest run date.

drop table if exists statistics, statistics_date;

create table statistics_date (run_date date);
create table statistics (
  institution text,
  admit_term  integer,
  event_pair  text,
  n           integer,
  median      double precision,
  siqr        double precision,
  mean        double precision,
  std_dev     double precision,
  conf_95     double precision,
  mode        integer,
  min         integer,
  max         integer,
  q1          double precision,
  q2          double precision,
  q3          double precision
)
