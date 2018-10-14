test_host <- Sys.getenv("DRILL_TEST_HOST", "localhost")

context("JDBC")
test_that("Core dbplyr ops work", {

  testthat::skip_on_cran()

  db <- src_drill_jdbc(test_host)

  expect_that(db, is_a("src_drill_jdbc"))

  test_dplyr <- tbl(db, "cp.`employee.json`")

  expect_that(test_dplyr, is_a("tbl"))
  expect_that(dplyr::count(test_dplyr, gender), is_a("tbl"))

})
