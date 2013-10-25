\name{accum.create.table}
\alias{accum.create.table}
\title{
accum.create.table: Create a table.
}
\description{
  Creates a table in the Accumulo instance.
}
\usage{
accum.create.table(tablename, timeType=c("LOGICAL","MILLIS"), login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablename}{
The name of the table to create.
}
  \item{timeType}{
The Accumulo timestamp type, one of LOGICAL or MILLIS.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the table is successfully created, error otherwise.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}
}
\examples{
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{accum.create.table("proxy-test","MILLIS")}
}
