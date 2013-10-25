\name{accum.flush.table}
\alias{accum.flush.table}
\title{
accum.flush.table: Flush a table to disk.
}
\description{
  Flushes all in-memory cells to disk for a table in the Accumulo instance.
}
\usage{
accum.flush.table(table,login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The table to flush.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if successful, error otherwise.
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
  \dontrun{accum.flush.table("test-proxy")}
}
