\name{accum.delete.table}
\alias{accum.delete.table}
\title{
accum.delete.table: Delete a table.
}
\description{
  Deletes a table in the Accumulo instance.
}
\usage{
accum.delete.table(tablename, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablename}{
The name of the table to delete.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the table is successfully deleted, error otherwise.
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
  \dontrun{accum.delete.table("proxy-test")}
}
