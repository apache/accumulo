\name{accum.rename.table}
\alias{accum.rename.table}
\title{
accum.rename.table: Rename a table.
}
\description{
  Renames a table in the Accumulo instance.
}
\usage{
accum.rename.table(oldname, newname, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{oldname}{
The name of the table to rename.
}
  \item{newname}{
The name to assign to the table.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the table is successfully renamed, error otherwise.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{accum.rename.table("proxy-test","test-proxy")}
}
