\name{accum.list.tablet.servers}
\alias{accum.list.tablet.servers}
\title{
accum.list.tablet.servers: List tablet servers.
}
\description{
  Provides a character vector of the tablet servers in the Accumulo instance.
}
\usage{
accum.list.tablet.servers(login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A character vector of tablet servers.
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
  \dontrun{accum.list.tablet.servers()}
}
