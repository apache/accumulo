\name{accum.update.and.flush}
\alias{accum.update.and.flush}
\title{
accum.update.and.flush: Update a table.
}
\description{
  Sends an update to a table in the Accumulo instance. Equivalent to calling
  accum.create.writer, accum.update, accum.flush.writer, and accum.close.writer in
  succession.
}
\usage{
accum.update.and.flush(table, cells, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The name of the table to update.
}
  \item{cells}{
A list of cell updates (returned from accum.create.cell).
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the update is successful, error otherwise.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}
\code{\link{accum.create.writer}}
\code{\link{accum.update}}
\code{\link{accum.flush.writer}}
\code{\link{accum.close.writer}}
\code{\link{accum.create.cell}}
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  updates <- list()
  updates[[1]] <- accum.create.cell("A123", "fam1", "qual1", "U&USA", 0, FALSE, "one")
  updates[[2]] <- accum.create.cell("A123", "fam1", "qual2", "U&USA", 0, FALSE, "two")
  updates[[3]] <- accum.create.cell("A123", "fam2", "qual3", "U&USA", 0, FALSE, "three")
  updates[[4]] <- accum.create.cell("C456", "fam1", "qual1", "U&USA", 0, FALSE, "ONE")
  updates[[5]] <- accum.create.cell("C456", "fam1", "qual2", "U&USA", 0, FALSE, "TWO")
  updates[[6]] <- accum.create.cell("C456", "fam2", "qual3", "U&USA", 0, FALSE, "THREE")
  \dontrun{accum.update.and.flush("test_proxy",updates)}
  
  

}
