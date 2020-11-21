library(sergeant.caffeinated)

test_host <- Sys.getenv("DRILL_TEST_HOST", "localhost")

be_quiet()

db <- dbConnect(drv = DrillJDBC(), sprintf("jdbc:drill:zk=%s", test_host))

dbCanConnect(drv = DrillJDBC(), sprintf("jdbc:drill:zk=%s", test_host))

dbExecute(db, "ALTER SESSION SET planner.cpu_load_average = 0.8")

test_dplyr <- tbl(db, "cp.`employee.json`")

expect_equal(inherits(test_dplyr, "tbl"), TRUE)

expect_equal(inherits(dplyr::count(test_dplyr, gender), "tbl"), TRUE)

