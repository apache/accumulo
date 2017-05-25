\name{accum.create.writer}
\alias{accum.create.writer}
\title{
accum.create.writer: Create a writer.
}
\description{
  Creates a writer to update a table in the Accumulo instance.
}
\usage{
accum.create.writer(table,opts=accum.create.writer.options(),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{table}{
The name of the table to update.
}
  \item{opts}{
A named list of options to pass to the writer. Use accum.create.writer.options() to create an appropriate list.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A pointer to a writer.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}
\code{\link{accum.create.writer.options}}
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{accum.create.writer("proxy-test")}
}
