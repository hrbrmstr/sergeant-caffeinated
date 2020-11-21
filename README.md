
[![Project Status: Active – The project has reached a stable, usable
state and is being actively
developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![Signed
by](https://img.shields.io/badge/Keybase-Verified-brightgreen.svg)](https://keybase.io/hrbrmstr)
![Signed commit
%](https://img.shields.io/badge/Signed_Commits-86%25-lightgrey.svg)
[![Linux build
Status](https://travis-ci.org/hrbrmstr/sergeant-caffeinated.svg?branch=master)](https://travis-ci.org/hrbrmstr/sergeant-caffeinated)
[![Coverage
Status](https://codecov.io/gh/hrbrmstr/sergeant-caffeinated/branch/master/graph/badge.svg)](https://codecov.io/gh/hrbrmstr/sergeant-caffeinated)
![Minimal R
Version](https://img.shields.io/badge/R%3E%3D-3.6.0-blue.svg)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

# sergeant-caffeinated

RJDBC Interface for Apache Drill

## Description

Apache Drill is a low-latency distributed query engine designed to
enable data exploration and analytics on both relational and
non-relational datastores, scaling to petabytes of data. An RDBC
interface with a thin set of ’dbplyr\` helper functions is provided.

## What’s Inside The Tin

The following functions are implemented:

-   `be_quiet`: Silence Java reflection warnings
-   `dbConnect,DrillJDBCDriver-method`: Connect to a schema/table
-   `dbGetRowsAffected,DrillJDBCResult-method`: Drill JDBC
    dbGetRowsAffected
-   `dbQuoteIdentifier,DrillJDBCConnection,character-method`: Thin
    wrapper for dbQuoteIdentifier
-   `dbSendQuery,DrillJDBCConnection,character-method`: Thin wrapper for
    dbSendQuery
-   `DrillJDBCDriver-class`: Create a Drill JDBC connection
-   `sergeant-caffeinated-exports`: sergeant exported operators
-   `sergeant.caffeinated`: Tools to Transform and Query Data with
    ‘Apache’ ‘Drill’
-   `sql_translate_env.DrillJDBCConnection`: Thin wrapper for
    sql\_translate\_env

## Installation

``` r
remotes::install_github("hrbrmstr/sergeant-caffeinated")
```

## Usage

``` r
library(sergeant.caffeinated)

# current version
packageVersion("sergeant.caffeinated")
## [1] '0.3.0'
```

``` r
library(tidyverse)

# use localhost if running standalone on same system otherwise the host or IP of your Drill server
test_host <- Sys.getenv("DRILL_TEST_HOST", "localhost")

be_quiet()

con <- dbConnect(drv = DrillJDBC(), sprintf("jdbc:drill:zk=%s", test_host))

db <- tbl(con, "cp.`employee.json`")

# without `collect()`:
db %>% 
  count(
    gender, 
    marital_status
  )
## # Source:   lazy query [?? x 3]
## # Database: DrillJDBCConnection
## # Groups:   gender
##   gender marital_status     n
##   <chr>  <chr>          <dbl>
## 1 F      S                297
## 2 M      M                278
## 3 M      S                276
## 4 F      M                304

db %>% 
  count(
    gender, 
    marital_status
  ) %>% 
  collect()
## # A tibble: 4 x 3
## # Groups:   gender [2]
##   gender marital_status     n
##   <chr>  <chr>          <dbl>
## 1 F      S                297
## 2 M      M                278
## 3 M      S                276
## 4 F      M                304

db %>% 
  group_by(position_title) %>% 
  count(gender) -> tmp2

group_by(db, position_title) %>% 
  count(gender) %>% 
  ungroup() %>% 
  mutate(
    full_desc = ifelse(gender=="F", "Female", "Male")
  ) %>% 
  collect() %>% 
  select(
    Title = position_title, 
    Gender = full_desc, 
    Count = n
  )
## # A tibble: 30 x 3
##    Title                  Gender Count
##    <chr>                  <chr>  <dbl>
##  1 President              Female     1
##  2 VP Country Manager     Male       3
##  3 VP Country Manager     Female     3
##  4 VP Information Systems Female     1
##  5 VP Human Resources     Female     1
##  6 Store Manager          Female    13
##  7 VP Finance             Male       1
##  8 Store Manager          Male      11
##  9 HQ Marketing           Female     2
## 10 HQ Information Systems Female     4
## # … with 20 more rows

arrange(db, desc(employee_id)) %>% print(n=20)
## # Source:     table<cp.`employee.json`> [?? x 16]
## # Database:   DrillJDBCConnection
## # Ordered by: desc(employee_id)
##    employee_id full_name first_name last_name position_id position_title store_id department_id birth_date hire_date
##          <dbl> <chr>     <chr>      <chr>           <dbl> <chr>             <dbl>         <dbl> <chr>      <chr>    
##  1        1156 Kris Sta… Kris       Stand              18 Store Tempora…       18            18 1914-02-02 1998-01-…
##  2        1155 Vivian B… Vivian     Burnham            18 Store Tempora…       18            18 1914-02-02 1998-01-…
##  3        1154 Judy Doo… Judy       Doolittle          18 Store Tempora…       18            18 1914-02-02 1998-01-…
##  4        1153 Gail Pir… Gail       Pirnie             18 Store Tempora…       18            18 1914-02-02 1998-01-…
##  5        1152 Barbara … Barbara    Younce             17 Store Permane…       18            17 1914-02-02 1998-01-…
##  6        1151 Burnis B… Burnis     Biltoft            17 Store Permane…       18            17 1914-02-02 1998-01-…
##  7        1150 Foster D… Foster     Detwiler           17 Store Permane…       18            17 1914-02-02 1998-01-…
##  8        1149 Bertha C… Bertha     Ciruli             17 Store Permane…       18            17 1914-02-02 1998-01-…
##  9        1148 Sharon B… Sharon     Bishop             16 Store Tempora…       18            16 1914-02-02 1998-01-…
## 10        1147 Jacqueli… Jacqueline Cutwright          16 Store Tempora…       18            16 1914-02-02 1998-01-…
## 11        1146 Elizabet… Elizabeth  Anderson           16 Store Tempora…       18            16 1914-02-02 1998-01-…
## 12        1145 Michael … Michael    Swartwood          16 Store Tempora…       18            16 1914-02-02 1998-01-…
## 13        1144 Shirley … Shirley    Curtsing…          15 Store Permane…       18            15 1914-02-02 1998-01-…
## 14        1143 Ana Quick Ana        Quick              15 Store Permane…       18            15 1914-02-02 1998-01-…
## 15        1142 Hazel So… Hazel      Souza              15 Store Permane…       18            15 1914-02-02 1998-01-…
## 16        1141 James Co… James      Compagno           15 Store Permane…       18            15 1914-02-02 1998-01-…
## 17        1140 Mona Jar… Mona       Jaramillo          13 Store Shift S…       18            11 1961-09-24 1998-01-…
## 18        1139 Jeanette… Jeanette   Belsey             12 Store Assista…       18            11 1972-05-12 1998-01-…
## 19        1138 James Ei… James      Eichorn            18 Store Tempora…       12            18 1914-02-02 1998-01-…
## 20        1137 Heather … Heather    Geiermann          18 Store Tempora…       12            18 1914-02-02 1998-01-…
## # … with more rows, and 6 more variables: salary <dbl>, supervisor_id <dbl>, education_level <chr>,
## #   marital_status <chr>, gender <chr>, management_role <chr>

db %>% 
  mutate(
    position_title = tolower(position_title),
    salary = as.numeric(salary),
    gender = ifelse(gender == "F", "Female", "Male"),
    marital_status = ifelse(marital_status == "S", "Single", "Married")
  ) %>%
  group_by(supervisor_id) %>% 
  summarise(
    underlings_count = n()
  ) %>% 
  collect()
## # A tibble: 112 x 2
##    supervisor_id underlings_count
##            <dbl>            <dbl>
##  1             0                1
##  2             1                7
##  3             5                9
##  4             4                2
##  5             2                3
##  6            20                2
##  7            21                4
##  8            22                7
##  9             6                4
## 10            36                2
## # … with 102 more rows
```

## sergeant Metrics

| Lang  | \# Files |  (%) | LoC |  (%) | Blank lines |  (%) | \# Lines |  (%) |
|:------|---------:|-----:|----:|-----:|------------:|-----:|---------:|-----:|
| R     |        5 | 0.23 | 161 | 0.21 |          31 | 0.20 |      114 | 0.37 |
| XML   |        1 | 0.05 |  66 | 0.09 |           0 | 0.00 |        0 | 0.00 |
| Maven |        1 | 0.05 |  65 | 0.08 |           6 | 0.04 |        4 | 0.01 |
| Rmd   |        1 | 0.05 |  52 | 0.07 |          26 | 0.17 |       31 | 0.10 |
| Java  |        2 | 0.09 |  32 | 0.04 |          11 | 0.07 |        7 | 0.02 |
| make  |        1 | 0.05 |  10 | 0.01 |           4 | 0.03 |        0 | 0.00 |
| SUM   |       11 | 0.50 | 386 | 0.50 |          78 | 0.50 |      156 | 0.50 |

clock Package Metrics for sergeant.caffeinated

## Code of Conduct

Please note that this project is released with a Contributor Code of
Conduct. By participating in this project you agree to abide by its
terms.
