#' Silence Java reflection warnings
#'
#' @export
be_quiet <- function() {
  rJava::J("is.rud.sergeant_caffeinated.App")$beQuiet()
}

#' @keywords internal
#' @export
dbplyr_edition.myConnectionClass <- function(con) 2L

#' @rdname DrillJDBC
#' @keywords internal
#' @export
setClass("DrillJDBCDriver", contains = "JDBCDriver")

#' Create a Drill JDBC connection
#'
#' @param identifier.quote quote
#' @param drill_jdbc_jar_path  path
#' @keywords internal
#' @export
DrillJDBC <- function(identifier.quote = "`", drill_jdbc_jar_path = Sys.getenv("DRILL_JDBC_JAR")) {

  driverClass <-  "org.apache.drill.jdbc.Driver"

  ## expand all paths in the classPath
  classPath <- path.expand(unlist(strsplit(drill_jdbc_jar_path, .Platform$path.sep)))

  ## this is benign in that it's equivalent to rJava::.jaddClassPath if a JVM is running
  rJava::.jinit(classPath)

  if (nchar(driverClass) && rJava::is.jnull(rJava::.jfindClass(as.character(driverClass)[1]))) {
    stop("Cannot find JDBC driver class ",driverClass)
  }

  jdrv <- rJava::.jnew(driverClass, check=FALSE)

  rJava::.jcheck(TRUE)

  if (rJava::is.jnull(jdrv)) jdrv <- rJava::.jnull()

  new("DrillJDBCDriver", identifier.quote = identifier.quote)

}

#' @rdname DrillJDBC
#' @keywords internal
#' @export
setClass("DrillJDBCConnection", contains = "JDBCConnection")

#' Connect to a schema/table
#'
#' @param drv driver
#' @param url connection string
#' @param user,password creds
#' @param ... extra params
#' @aliases dbConnect
#' @export
setMethod(
  f = "dbConnect",
  signature = "DrillJDBCDriver",
  definition = function(drv, url, user='', password='', ...) {
    res <- callNextMethod(drv, url, user, password, ...)
    cls <- "DrillJDBCConnection"
    attr(cls, "package") <- "sergeant.caffeinated"
    class(res) <- cls
    return(res)
  }
)

quote_identifier <- function(conn, x, ...) {
  ifelse(grepl("`", x), dbplyr::sql_quote(x, ' '), dbplyr::sql_quote(x, '`'))
}

#' Thin wrapper for dbQuoteIdentifier
#'
#' @param conn connection
#' @param x, item to quote
#' @param ... passed on to downstream methods
#' @aliases dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("DrillJDBCConnection", "character"),
  quote_identifier
)

#' @rdname DrillJDBC
#' @keywords internal
#' @export
setClass("DrillJDBCResult", contains = "JDBCResult")

#' Drill JDBC dbGetRowsAffected
#'
#' @param res A \code{\linkS4class{DrillJDBCResult}} object
#' @param ... Extra optional parameters
#' @aliases dbGetRowsAffected
#' @export
setMethod(
  f = "dbGetRowsAffected",
  signature = "DrillJDBCResult",
  definition = function(res, ...) {
    nr <- 0
    while(res@jr$`next`()) nr <- nr+1
    return(nr)
  }
)

#' Thin wrapper for dbSendQuery
#'
#' @param conn connection
#' @param statement SQL statement,
#' @param ... passed on
#' @param list list
#' @aliases dbSendQuery
#' @export
setMethod(
  f = "dbSendQuery",
  signature = signature(conn="DrillJDBCConnection", statement="character"),
  definition = function(conn, statement, ..., list=NULL) {
    res <- callNextMethod(conn, statement, ..., list=list)
    cls <- "DrillJDBCResult"
    attr(cls, "package") <- "sergeant.caffeinated"
    class(res) <- cls
    res
  }
)

#' Thin wrapper for sql_translate_env
#'
#' @param con connection
#' @keywords internal
#' @export
sql_translate_env.DrillJDBCConnection <- function(con) {

  x <- con

  dbplyr::sql_variant(

    scalar = dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      `!=` = dbplyr::sql_infix("<>"),
      as.numeric = function(x) build_sql("CAST(", x, " AS DOUBLE)"),
      as.character = function(x) build_sql("CAST(", x, " AS CHARACTER)"),
      as.date = function(x) build_sql("CAST(", x, " AS DATE)"),
      as.posixct = function(x) build_sql("CAST(", x, " AS TIMESTAMP)"),
      as.logical = function(x) build_sql("CAST(", x, " AS BOOLEAN)"),
      date_part = function(x, y) build_sql("DATE_PART(", x, ",", y ,")"),
      grepl = function(x, y) build_sql("CONTAINS(", y, ", ", x, ")"),
      gsub = function(x, y, z) build_sql("REGEXP_REPLACE(", z, ", ", x, ",", y ,")"),
      str_replace = function(x, y, z) build_sql("REGEXP_REPLACE(", x, ", ", y, ",", z ,")"),
      trimws = function(x) build_sql("TRIM(both ' ' FROM ", x, ")"),
      cbrt = sql_prefix("CBRT", 1),
      degrees = sql_prefix("DEGREES", 1),
      e = sql_prefix("E", 0),
      row_number = sql_prefix("row_number", 0),
      lshift = sql_prefix("LSHIFT", 2),
      mod = sql_prefix("MOD", 2),
      age = sql_prefix("AGE", 1),
      negative = sql_prefix("NEGATIVE", 1),
      pi = sql_prefix("PI", 0),
      pow = sql_prefix("POW", 2),
      radians = sql_prefix("RADIANS", 1),
      rand = sql_prefix("RAND", 0),
      rshift = sql_prefix("RSHIFT", 2),
      trunc = sql_prefix("TRUNC", 2),
      contains = sql_prefix("CONTAINS", 2),
      convert_to = sql_prefix("CONVERT_TO", 2),
      convert_from = sql_prefix("CONVERT_FROM", 2),
      string_binary = sql_prefix("STRING_BINARY", 1),
      binary_string = sql_prefix("BINARY_STRING", 1),
      to_char = sql_prefix("TO_CHAR", 2),
      to_date = sql_prefix("TO_DATE", 2),
      to_number = sql_prefix("TO_NUMBER", 2),
      char_to_timestamp = sql_prefix("TO_TIMESTAMP", 2),
      double_to_timestamp = sql_prefix("TO_TIMESTAMP", 1),
      char_length = sql_prefix("CHAR_LENGTH", 1),
      flatten = sql_prefix("FLATTEN", 1),
      kvgen = sql_prefix("KVGEN", 1),
      repeated_count = sql_prefix("REPEATED_COUNT", 1),
      repeated_contains = sql_prefix("REPEATED_CONTAINS", 2),
      ilike = sql_prefix("ILIKE", 2),
      init_cap = sql_prefix("INIT_CAP", 1),
      length = sql_prefix("LENGTH", 1),
      lower = sql_prefix("LOWER", 1),
      str_to_lower = sql_prefix("LOWER", 1),
      tolower = sql_prefix("LOWER", 1),
      ltrim = sql_prefix("LTRIM", 2),
      nullif = sql_prefix("NULLIF", 2),
      position = function(x, y) build_sql("POSITION(", x, " IN ", y, ")"),
      regexp_replace = sql_prefix("REGEXP_REPLACE", 3),
      rtrim = sql_prefix("RTRIM", 2),
      rpad = sql_prefix("RPAD", 2),
      rpad_with = sql_prefix("RPAD", 3),
      lpad = sql_prefix("LPAD", 2),
      lpad_with = sql_prefix("LPAD", 3),
      strpos = sql_prefix("STRPOS", 2),
      substr = sql_prefix("SUBSTR", 3),
      str_sub = sql_prefix("SUBSTR", 3),
      trim = function(x, y, z) build_sql("TRIM(", x, " ", y, " FROM ", z, ")"),
      upper = sql_prefix("UPPER", 1),
      str_to_upper = sql_prefix("UPPER", 1),
      toupper = sql_prefix("UPPER", 1)
    ),

    aggregate = dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("COUNT(*)"),
      cor = dbplyr::sql_prefix("CORR"),
      cov = dbplyr::sql_prefix("COVAR_SAMP"),
      sd =  dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      n_distinct = function(x) {
        dbplyr::build_sql(dbplyr::sql("COUNT(DISTINCT "), x, dbplyr::sql(")"))
      }
    ),

    window = dbplyr::sql_translator(
      .parent = dbplyr::base_win,
      n = function() { dbplyr::win_over(dbplyr::sql("count(*)"),
                                        partition = dbplyr::win_current_group()) },
      cor = dbplyr::win_recycled("corr"),
      cov = dbplyr::win_recycled("covar_samp"),
      sd =  dbplyr::win_recycled("stddev_samp"),
      var = dbplyr::win_recycled("var_samp"),
      all = dbplyr::win_recycled("bool_and"),
      any = dbplyr::win_recycled("bool_or")
    )

  )

}
