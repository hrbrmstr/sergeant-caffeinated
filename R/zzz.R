.onLoad <- function(libname, pkgname) {
  rJava::.jpackage(pkgname, lib.loc = libname)
  rJava::.jaddClassPath(system.file("java", "RJDBC.jar", package = "RJDBC"))
  rJava::J("java.util.logging.LogManager")$getLogManager()$reset()
}
