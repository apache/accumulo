\name{accum.list.splits}
\alias{accum.list.splits}
\title{
accum.list.splits: List table splits
}
\description{
  Provides a character vector of the split points for a table in the Accumulo instance.
}
\usage{
accum.list.splits(table,maxsplits=9999,login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The table to list the split points for.
}
  \item{maxsplits}{
The maximum number of split points to return.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A character vector of table split points.
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
  \dontrun{accum.list.splits("test-proxy")}
}
