\name{accum.table.exists}
\alias{accum.table.exists}
\title{
accum.table.exists: Check table existence.
}
\description{
  Checks to see if the specified table exists in the Accumulo instance.
}
\usage{
accum.table.exists(tablename, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablename}{
The name of the table to test the existence of.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the table exists, FALSE otherwise.
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
  \dontrun{accum.table.exists("proxy-test")}
}
