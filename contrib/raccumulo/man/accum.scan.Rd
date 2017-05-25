\name{accum.scan}
\alias{accum.scan}
\title{
accum.scan: Scan a table.
}
\description{
  Scans a table and prints results in a format similar to the no-argument scan function of the Accumulo shell.
}
\usage{
accum.scan(table, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The name of the table to scan.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the scan is successful, error otherwise.
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
  \dontrun{accum.scan("proxy-test")}
}
