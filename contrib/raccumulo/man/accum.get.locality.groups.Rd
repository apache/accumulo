\name{accum.get.locality.groups}
\alias{accum.get.locality.groups}
\title{
accum.get.locality.groups: List a table's locality groups
}
\description{
  Provides a list of the locality groups for a table in the Accumulo instance.
}
\usage{
accum.get.locality.groups(table,login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The table to list the locality groups for.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A list of table locality groups.
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
  \dontrun{accum.get.locality.groups("test-proxy")}
}
