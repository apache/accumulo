\name{accum.online.table}
\alias{accum.online.table}
\title{
accum.online.table
}
\description{
  Sets a table in the Accumulo instance to online mode.
}
\usage{
accum.online.table(tablename, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablename}{
The name of the table to set online.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the table is successfully set to online, error otherwise.
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
  \dontrun{accum.online.table("proxy-test")}
}
