\name{accum.create.scanner}
\alias{accum.create.scanner}
\title{
accum.create.scanner: Create a scanner.
}
\description{
  Creates a scanner to query a table in the Accumulo instance.
}
\usage{
accum.create.scanner(table, opts=list(), login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The name of the table to scan.
}
  \item{opts}{
A named list of options to pass to the scanner.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A pointer to a scanner.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}
\code{\link{accum.create.scan.options}}
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{accum.create.scanner("proxy-test")}
}
