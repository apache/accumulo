\name{accum.add.splits}
\alias{accum.add.splits}
\title{
accum.add.splits: Add split points to a table.
}
\description{
  Adds split points to a table in the Accumulo instance.
}
\usage{
accum.add.splits(table, splits, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The name of the table add splits to.
}
  \item{splits}{
A character vector of split points.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the splits are successfully added, error otherwise.
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
  \dontrun{accum.add.splits("proxy-test",c("B","H","O","U")}
}
