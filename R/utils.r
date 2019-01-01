.verify.JDBC.result <- function(result, ..., conn=NULL) {

  if (rJava::is.jnull(result)) {

    x <- rJava::.jgetEx(TRUE)

    if (rJava::is.jnull(x)) {
      stop(...)
    } else if (is.null(conn)) {
      stop(...)
    } else {

      jr <- unlist(rJava::.jcall(x, "S", "getMessage"))

      resp <- unlist(list(...))

      jdbc_err <- resp[1]
      oq <- resp[2]

      resp <- unlist(strsplit(jr, "\n"))

      err <- resp[grepl("Error Id", resp)]

      resp <- resp[resp != ""]
      resp <- resp[!grepl("Error Id", resp)]

      err <- sub("^.*: ", "", err)
      err <- unlist(strsplit(err, "[[:space:]]+"))[1]

      oq <- unlist(strsplit(oq, "\n"))

      c(
        sprintf("%s:\n", jdbc_err),
        sprintf("%3d: %s", 1:length(oq), oq),
        " ",
        resp,
        sprintf(
          "\nQuery Profile Error Id: %s", err
        )
      ) -> resp

      resp <- paste0(resp, collapse="\n")

      stop(resp, call.=FALSE)

    }

  }

}


try_require <- function(package, fun) {
  if (requireNamespace(package, quietly = TRUE)) {
    library(package, character.only = TRUE)
    return(invisible())
  }

  stop("Package `", package, "` required for `", fun , "`.\n", # nocov start
       "Please install and try again.", call. = FALSE) # nocov end
}

