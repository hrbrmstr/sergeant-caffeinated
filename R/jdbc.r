.fillStatementParameters <- function(s, l) {
  for (i in 1:length(l)) {
    v <- l[[i]]
    if (is.na(v)) { # map NAs to NULLs (courtesy of Axel Klenk)
      sqlType <- if (is.integer(v)) 4 else if (is.numeric(v)) 8 else 12
      rJava::.jcall(s, "V", "setNull", i, as.integer(sqlType))
    } else if (is.integer(v))
      rJava::.jcall(s, "V", "setInt", i, v[1])
    else if (is.numeric(v))
      rJava::.jcall(s, "V", "setDouble", i, as.double(v)[1])
    else
      rJava::.jcall(s, "V", "setString", i, as.character(v)[1])
  }
}

#' JDBC Driver for Drill database.
#'
#' @keywords internal
#' @export
setClass(
  Class = "DrillJDBCDriver",
  contains = "JDBCDriver"
)

#' Drill JDBC connection class.
#'
#' @export
#' @keywords internal
#' @export
setClass(
  Class = "DrillJDBCConnection",
  contains = "JDBCConnection"
)

#' Connect to Drill JDBC with your own connection string
#'
#' You should really use [drill_jdbc()] as it handles some cruft for
#' you, but you can specify the full JDBC connection string
#'
#' @md
#' @family Drill JDBC API
#' @param drv what you get back from [DrillJDBC()]
#' @param url your Drill connection strinfg
#' @param user,password username & password (leave as-is for no-auth)
#' @param ... additional `name=val` properties which will be set with Java's
#'        `SetProperty` method.
#' @export
setMethod(
  f = "dbConnect",
  signature = "DrillJDBCDriver",
  definition = function(drv, url, user='', password='', ...) {

    rJava::.jcall(
      "java/sql/DriverManager",
      "Ljava/sql/Connection;",
      "getConnection",
      as.character(url)[1],
      as.character(user)[1],
      as.character(password)[1],
      check = FALSE
    ) -> jc

    if (rJava::is.jnull(jc) && !rJava::is.jnull(drv@jdrv)) {
      # ok one reason for this to fail is its interaction with rJava's
      # class loader. In that case we try to load the driver directly.
      oex <- rJava::.jgetEx(TRUE)

      p <- rJava::.jnew("java/util/Properties")

      if (length(user)==1 && nchar(user)) {
        rJava::.jcall(p,"Ljava/lang/Object;","setProperty","user",user)
      }

      if (length(password)==1 && nchar(password)) {
        rJava::.jcall(p,"Ljava/lang/Object;","setProperty","password",password)
      }

      l <- list(...)
      if (length(names(l))) for (n in names(l)) {
        rJava::.jcall(p, "Ljava/lang/Object;", "setProperty", n, as.character(l[[n]]))
      }

      jc <- rJava::.jcall(drv@jdrv, "Ljava/sql/Connection;", "connect", as.character(url)[1], p)

    }

    .verify.JDBC.result(jc, "Unable to connect JDBC to ",url, , conn=NULL)

    new("DrillJDBCConnection", jc=jc, identifier.quote=drv@identifier.quote)

  },

  valueClass = "DrillJDBCConnection"

)

#' Drill JDBC dbDataType
#'
#' @param dbObj A \code{\linkS4class{DrillJDBCDriver}} object
#' @param obj Any R object
#' @param ... Extra optional parameters
#' @family Drill JDBC API
#' @export
setMethod(
  "dbDataType",
  "DrillJDBCConnection",
  function(dbObj, obj, ...) {
    if (is.integer(obj)) "INTEGER"
    else if (inherits(obj, "Date")) "DATE"
    else if (identical(class(obj), "times")) "TIME"
    else if (inherits(obj, "POSIXct")) "TIMESTAMP"
    else if (is.numeric(obj)) "DOUBLE"
    else "VARCHAR(255)"
  },
  valueClass = "character"
)


#' Drill's JDBC driver main class loader
#'
#' @family Drill JDBC API
#' @export
DrillJDBC <- function() {

  driverClass <-  "org.apache.drill.jdbc.Driver"

  ## expand all paths in the classPath
  classPath <- path.expand(unlist(strsplit(Sys.getenv("DRILL_JDBC_JAR"), .Platform$path.sep)))

  ## this is benign in that it's equivalent to rJava::.jaddClassPath if a JVM is running
  rJava::.jinit(classPath)

  rJava::.jaddClassPath(system.file("java", "RJDBC.jar", package="RJDBC"))
  rJava::.jaddClassPath(system.file("java", "slf4j-nop-1.7.25.jar", package = "sergeant.caffeinated"))

  if (nchar(driverClass) && rJava::is.jnull(rJava::.jfindClass(as.character(driverClass)[1]))) {
    stop("Cannot find JDBC driver class ",driverClass)
  }

  jdrv <- rJava::.jnew(driverClass, check=FALSE)

  rJava::.jcheck(TRUE)

  if (rJava::is.jnull(jdrv)) jdrv <- rJava::.jnull()

  new("DrillJDBCDriver", identifier.quote = "`", jdrv = jdrv)

}

#' Connect to Drill using JDBC
#'
#' The DRILL JDBC driver fully-qualified path must be placed in the
#' \code{DRILL_JDBC_JAR} environment variable. This is best done via \code{~/.Renviron}
#' for interactive work. e.g. \code{DRILL_JDBC_JAR=/usr/local/drill/jars/jdbc-driver/drill-jdbc-all-1.10.0.jar}
#'
#' [src_drill_jdbc()] wraps the JDBC [dbConnect()] connection instantation in
#' [dbplyr::src_dbi()] to return the equivalent of the REST driver's [src_drill()].
#'
#' @param nodes character vector of nodes. If more than one node, you can either have
#'              a single string with the comma-separated node:port pairs pre-made or
#'              pass in a character vector with multiple node:port strings and the
#'              function will make a comma-separated node string for you.
#' @param cluster_id the cluster id from \code{drill-override.conf}
#' @param schema an optional schema name to append to the JDBC connection string
#' @param use_zk are you connecting to a ZooKeeper instance (default: \code{TRUE}) or
#'               connecting to an individual DrillBit.
#' @family Drill JDBC API
#' @return a JDBC connection object
#' @references \url{https://drill.apache.org/docs/using-the-jdbc-driver/#using-the-jdbc-url-for-a-random-drillbit-connection}
#' @export
#' @examples \dontrun{
#' con <- drill_jdbc("localhost:2181", "main")
#' drill_query(con, "SELECT * FROM cp.`employee.json`")
#'
#' # you can also use the connection with RJDBC calls:
#' dbGetQuery(con, "SELECT * FROM cp.`employee.json`")
#'
#' # for local/embedded mode with default configuration info
#' con <- drill_jdbc("localhost:31010", use_zk=FALSE)
#' }
drill_jdbc <- function(nodes = "localhost:2181", cluster_id = NULL,
                       schema = NULL, use_zk = TRUE) {

  try_require("rJava")
  try_require("RJDBC")

  jar_path <- Sys.getenv("DRILL_JDBC_JAR")
  if (!file.exists(jar_path)) {
    stop(sprintf("Cannot locate DRILL JDBC JAR [%s]", jar_path))
  }

  drill_jdbc_drv <- DrillJDBC()

  conn_type <- "drillbit"
  if (use_zk) conn_type <- "zk"

  if (length(nodes) > 1) nodes <- paste0(nodes, collapse=",")

  conn_str <- sprintf("jdbc:drill:%s=%s", conn_type, nodes)

  if (!is.null(cluster_id)) {
    conn_str <- sprintf("%s%s", conn_str, sprintf("/drill/%s", cluster_id))
  }

  if (!is.null(schema)) conn_str <- sprintf("%s;%s", schema)

  message(sprintf("Using [%s]...", conn_str))

  dbConnect(drill_jdbc_drv, conn_str)

}

#' @rdname drill_jdbc
#' @family Drill JDBC API
#' @export
src_drill_jdbc <- function(nodes = "localhost:2181", cluster_id = NULL,
                           schema = NULL, use_zk = TRUE) {

  con <- drill_jdbc(nodes, cluster_id, schema, use_zk)
  src_sql("drill_jdbc", con)

}

#' @rdname drill_jdbc
#' @param src A Drill "src" created with \code{src_drill()}
#' @param from A Drill view or table specification
#' @param ... Extra parameters
#' @family Drill JDBC API
#' @export
tbl.src_drill_jdbc <- function(src, from, ...) {
  tbl_sql("drill_jdbc", src=src, from=from, ...)
}

#' Drill internals
#'
#' @rdname drill_jdbc_internals
#' @keywords internal
#' @export
db_data_type.DrillJDBCConnection <- function(con, fields, ...) {
  data_type <- function(x) {
    switch(
      class(x)[1],
      integer64 = "BIGINT",
      logical = "BOOLEAN",
      integer = "INTEGER",
      numeric = "DOUBLE",
      factor =  "CHARACTER",
      character = "CHARACTER",
      Date = "DATE",
      POSIXct = "TIMESTAMP",
      stop("Can't map type ", paste(class(x), collapse = "/"),
           " to a supported database type.")
    )
  }
  vapply(fields, data_type, character(1))
}

#' Drill internals
#'
#' @rdname drill_jdbc_internals
#' @keywords internal
#' @export
db_data_type.tbl_drill_jdbc <- db_data_type.DrillJDBCConnection

#' @rdname drill_jdbc_internals
#' @keywords internal
#' @export
setClass("DrillJDBCResult", representation("JDBCResult", jr="jobjRef", md="jobjRef", stat="jobjRef", pull="jobjRef"))

#' @rdname drill_jdbc_internals
#' @keywords internal
#' @export
setMethod(
  f = "dbSendQuery",
  signature = signature(conn="DrillJDBCConnection", statement="character"),
  definition = function(conn, statement, ..., list=NULL) {
    statement <- as.character(statement)[1L]
    ## if the statement starts with {call or {?= call then we use CallableStatement
    if (isTRUE(as.logical(grepl("^\\{(call|\\?= *call)", statement)))) {
      s <- rJava::.jcall(conn@jc, "Ljava/sql/CallableStatement;", "prepareCall", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC callable statement ",statement, conn=conn)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement, conn=conn)
    } else if (length(list(...)) || length(list)) { ## use prepared statements if there are additional arguments
      s <- rJava::.jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", statement, check=FALSE)
      .verify.JDBC.result(s, "Unable to execute JDBC prepared statement ", statement, conn=conn)
      if (length(list(...))) .fillStatementParameters(s, list(...))
      if (!is.null(list)) .fillStatementParameters(s, list)
      r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery", check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement, conn=conn)
    } else { ## otherwise use a simple statement some DBs fail with the above)
      s <- rJava::.jcall(conn@jc, "Ljava/sql/Statement;", "createStatement")
      .verify.JDBC.result(s, "Unable to create simple JDBC statement ",statement, conn=conn)
      r <- rJava::.jcall(s, "Ljava/sql/ResultSet;", "executeQuery", as.character(statement)[1], check=FALSE)
      .verify.JDBC.result(r, "Unable to retrieve JDBC result set for ",statement, conn=conn)
    }
    md <- rJava::.jcall(r, "Ljava/sql/ResultSetMetaData;", "getMetaData", check=FALSE)
    .verify.JDBC.result(md, "Unable to retrieve JDBC result set meta data for ",statement, " in dbSendQuery")
    new("DrillJDBCResult", jr=r, md=md, stat=s, pull=rJava::.jnull())
  })

#' @rdname drill_jdbc_internals
#' @keywords internal
#' @export
sql_escape_ident.DrillJDBCConnection <- function(con, x) {
  ifelse(grepl(con@identifier.quote, x), sql_quote(x, ' '), sql_quote(x, con@identifier.quote))
}

#' @rdname drill_jdbc_internals
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

#' src tbls
#'
#' "SHOW DATABASES"
#'
#' @rdname drill_jdbc_internals
#' @family Drill JDBC API
#' @keywords internal
#' @param x x
#' @export
src_tbls.src_dbi <- function(x) {
  tmp <- dbGetQuery(x$con, "SHOW DATABASES")
  paste0(unlist(tmp$SCHEMA_NAME, use.names=FALSE), collapse=", ")
}
