\name{accum.get.table.properties}
\alias{accum.get.table.properties}
\title{
accum.get.table.properties: List a table's properties.
}
\description{
  Provides a list of the properties for a table in the Accumulo instance.
}
\usage{
accum.get.table.properties(table,login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The table to list the properties for.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A list of table properties.
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
  \dontrun{accum.get.table.properties("test-proxy")}
}
