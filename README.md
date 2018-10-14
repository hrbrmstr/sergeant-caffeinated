
<!-- README.md is generated from README.Rmd. Please edit that file -->

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1248912.svg)](https://doi.org/10.5281/zenodo.1248912)
[![Travis-CI Build
Status](https://travis-ci.org/hrbrmstr/sergeant-caffeinated.svg?branch=master)](https://travis-ci.org/hrbrmstr/sergeant-caffeinated)
[![Coverage
Status](https://codecov.io/gh/hrbrmstr/sergeant-caffeinated/branch/master/graph/badge.svg)](https://codecov.io/gh/hrbrmstr/sergeant-caffeinated)
[![CRAN\_Status\_Badge](http://www.r-pkg.org/badges/version/sergeant-caffeinated)](https://cran.r-project.org/package=sergeant-caffeinated)

# 💂☕️ sergeant.caffeinated

Tools to Transform and Query Data with ‘Apache’ ‘Drill’ (JDBC)

## NOTE

This is the Java/JDBC-interface to Apache Drill. For non-Java/JDBC, see
the `sergeant` package ([GitLab](https://gitlab.com/hrbrmstr/sergeant/);
[GitHub](https://github.com/hrbrmstr/sergeant/)).

## Description

Drill + `sergeant` is (IMO) a streamlined alternative to Spark +
`sparklyr` if you don’t need the ML components of Spark (i.e. just need
to query “big data” sources, need to interface with parquet, need to
combine disparate data source types — json, csv, parquet, rdbms - for
aggregation, etc). Drill also has support for spatial queries.

Using Drill SQL queries that reference parquet files on a local linux or
macOS workstation can often be more performant than doing the same data
ingestion & wrangling work with R (especially for large or disperate
data sets). Drill can often help further streaming workflows that
infolve wrangling many tiny JSON files on a daily basis.

Drill can be obtained from <https://drill.apache.org/download/> (use
“Direct File Download”). Drill can also be installed via
[Docker](https://drill.apache.org/docs/running-drill-on-docker/). For
local installs on Unix-like systems, a common/suggestion location for
the Drill directory is `/usr/local/drill` as the install directory.

Drill embedded (started using the `$DRILL_BASE_DIR/bin/drill-embedded`
script) is a super-easy way to get started playing with Drill on a
single workstation and most of many workflows can “get by” using Drill
this way.

The following functions are implemented:

**`DBI`** (RJDBC)

  - `drill_jdbc`: Connect to Drill using JDBC, enabling use of said
    idioms. See `RJDBC` for more info.

NOTE: The DRILL JDBC driver fully-qualified path must be placed in the
`DRILL_JDBC_JAR` environment variable. This is best done via
`~/.Renviron` for interactive work. i.e.
`DRILL_JDBC_JAR=/usr/local/drill/jars/drill-jdbc-all-1.14.0.jar`

**`dplyr`**: (RJDBC)

  - `src_drill_jdbc`: Connect to Drill (using dplyr & RJDBC) +
    supporting
functions

## Installation

``` r
devtools::install_git("https://gitlab.com/hrbrmstr/sergeant-caffeinated")
# OF
devtools::install_github("hrbrmstr/sergeant-caffeinated")
```

## Usage

``` r
library(sergeant.caffeinated)
library(tidyverse)

# use localhost if running standalone on same system otherwise the host or IP of your Drill server
ds <- src_drill_jdbc("localhost")  #ds
db <- tbl(ds, "cp.`employee.json`") 

# without `collect()`:
count(db, gender, marital_status)
## # Source:   lazy query [?? x 3]
## # Database: DrillJDBCConnection
## # Groups:   gender
##   gender marital_status     n
##   <chr>  <chr>          <dbl>
## 1 F      S               297.
## 2 M      M               278.
## 3 M      S               276.
## 4 F      M               304.

count(db, gender, marital_status) %>% collect()
## # A tibble: 4 x 3
## # Groups:   gender [2]
##   gender marital_status     n
## * <chr>  <chr>          <dbl>
## 1 F      S               297.
## 2 M      M               278.
## 3 M      S               276.
## 4 F      M               304.

group_by(db, position_title) %>% 
  count(gender) -> tmp2

group_by(db, position_title) %>% 
  count(gender) %>% 
  ungroup() %>% 
  mutate(full_desc=ifelse(gender=="F", "Female", "Male")) %>% 
  collect() %>% 
  select(Title=position_title, Gender=full_desc, Count=n)
## # A tibble: 30 x 3
##    Title                  Gender Count
##  * <chr>                  <chr>  <dbl>
##  1 President              Female    1.
##  2 VP Country Manager     Male      3.
##  3 VP Country Manager     Female    3.
##  4 VP Information Systems Female    1.
##  5 VP Human Resources     Female    1.
##  6 Store Manager          Female   13.
##  7 VP Finance             Male      1.
##  8 Store Manager          Male     11.
##  9 HQ Marketing           Female    2.
## 10 HQ Information Systems Female    4.
## # ... with 20 more rows

arrange(db, desc(employee_id)) %>% print(n=20)
## # Source:     table<cp.`employee.json`> [?? x 16]
## # Database:   DrillJDBCConnection
## # Ordered by: desc(employee_id)
##    employee_id full_name  first_name last_name  position_id position_title store_id department_id birth_date hire_date 
##          <dbl> <chr>      <chr>      <chr>            <dbl> <chr>             <dbl>         <dbl> <chr>      <chr>     
##  1       1156. Kris Stand Kris       Stand              18. Store Tempora…      18.           18. 1914-02-02 1998-01-0…
##  2       1155. Vivian Bu… Vivian     Burnham            18. Store Tempora…      18.           18. 1914-02-02 1998-01-0…
##  3       1154. Judy Dool… Judy       Doolittle          18. Store Tempora…      18.           18. 1914-02-02 1998-01-0…
##  4       1153. Gail Pirn… Gail       Pirnie             18. Store Tempora…      18.           18. 1914-02-02 1998-01-0…
##  5       1152. Barbara Y… Barbara    Younce             17. Store Permane…      18.           17. 1914-02-02 1998-01-0…
##  6       1151. Burnis Bi… Burnis     Biltoft            17. Store Permane…      18.           17. 1914-02-02 1998-01-0…
##  7       1150. Foster De… Foster     Detwiler           17. Store Permane…      18.           17. 1914-02-02 1998-01-0…
##  8       1149. Bertha Ci… Bertha     Ciruli             17. Store Permane…      18.           17. 1914-02-02 1998-01-0…
##  9       1148. Sharon Bi… Sharon     Bishop             16. Store Tempora…      18.           16. 1914-02-02 1998-01-0…
## 10       1147. Jacquelin… Jacqueline Cutwright          16. Store Tempora…      18.           16. 1914-02-02 1998-01-0…
## 11       1146. Elizabeth… Elizabeth  Anderson           16. Store Tempora…      18.           16. 1914-02-02 1998-01-0…
## 12       1145. Michael S… Michael    Swartwood          16. Store Tempora…      18.           16. 1914-02-02 1998-01-0…
## 13       1144. Shirley C… Shirley    Curtsinger         15. Store Permane…      18.           15. 1914-02-02 1998-01-0…
## 14       1143. Ana Quick  Ana        Quick              15. Store Permane…      18.           15. 1914-02-02 1998-01-0…
## 15       1142. Hazel Sou… Hazel      Souza              15. Store Permane…      18.           15. 1914-02-02 1998-01-0…
## 16       1141. James Com… James      Compagno           15. Store Permane…      18.           15. 1914-02-02 1998-01-0…
## 17       1140. Mona Jara… Mona       Jaramillo          13. Store Shift S…      18.           11. 1961-09-24 1998-01-0…
## 18       1139. Jeanette … Jeanette   Belsey             12. Store Assista…      18.           11. 1972-05-12 1998-01-0…
## 19       1138. James Eic… James      Eichorn            18. Store Tempora…      12.           18. 1914-02-02 1998-01-0…
## 20       1137. Heather G… Heather    Geiermann          18. Store Tempora…      12.           18. 1914-02-02 1998-01-0…
## # ... with more rows, and 6 more variables: salary <dbl>, supervisor_id <dbl>, education_level <chr>,
## #   marital_status <chr>, gender <chr>, management_role <chr>

mutate(db, position_title=tolower(position_title)) %>%
  mutate(salary=as.numeric(salary)) %>% 
  mutate(gender=ifelse(gender=="F", "Female", "Male")) %>%
  mutate(marital_status=ifelse(marital_status=="S", "Single", "Married")) %>% 
  group_by(supervisor_id) %>% 
  summarise(underlings_count=n()) %>% 
  collect()
## # A tibble: 112 x 2
##    supervisor_id underlings_count
##  *         <dbl>            <dbl>
##  1            0.               1.
##  2            1.               7.
##  3            5.               9.
##  4            4.               2.
##  5            2.               3.
##  6           20.               2.
##  7           21.               4.
##  8           22.               7.
##  9            6.               4.
## 10           36.               2.
## # ... with 102 more rows
```

\`\`\` \#\#\# Test Results

``` r
library(sergeant.caffeinated)
library(testthat)
## 
## Attaching package: 'testthat'
## The following object is masked from 'package:dplyr':
## 
##     matches
## The following object is masked from 'package:purrr':
## 
##     is_null

date()
## [1] "Sun Oct 14 09:31:23 2018"

devtools::test()
## Loading sergeant.caffeinated
## Testing sergeant.caffeinated
## ✔ | OK F W S | Context
## 
⠏ |  0       | JDBC
⠋ |  1       | JDBC
⠙ |  2       | JDBC
⠹ |  3       | JDBC
✔ |  3       | JDBC [0.3 s]
## 
## ══ Results ════════════════════════════════════════════════════════════════
## Duration: 0.3 s
## 
## OK:       3
## Failed:   0
## Warnings: 0
## Skipped:  0
```

## sergeant Metrics

| Lang | \# Files |  (%) | LoC | (%) | Blank lines |  (%) | \# Lines |  (%) |
| :--- | -------: | ---: | --: | --: | ----------: | ---: | -------: | ---: |
| R    |        7 | 0.88 | 302 | 0.9 |          78 | 0.68 |      164 | 0.77 |
| Rmd  |        1 | 0.12 |  35 | 0.1 |          37 | 0.32 |       48 | 0.23 |

## Code of Conduct

Please note that this project is released with a [Contributor Code of
Conduct](CONDUCT.md). By participating in this project you agree to
abide by its terms.
