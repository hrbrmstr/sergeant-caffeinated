.verify.JDBC.result <- function (result, ...) {
  if (rJava::is.jnull(result)) {
    x <- rJava::.jgetEx(TRUE)
    if (rJava::is.jnull(x))
      stop(...)
    else
      stop(...," (",rJava::.jcall(x, "S", "getMessage"),")")
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

