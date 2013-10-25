\name{accum.get.active.scans}
\alias{accum.get.active.scans}
\title{
accum.get.active.scans: List active scans.
}
\description{
  Provides a character vector of the active scans on a tablet in the Accumulo instance.
}
\usage{
accum.get.active.scans(tablet, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablet}{
The tablet server to get active scans for.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A character vector of active scans.
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
  \dontrun{accum.get.active.scans("127.0.0.1:9997")}
}
