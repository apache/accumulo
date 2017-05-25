\name{accum.set.table.property}
\alias{accum.set.table.property}
\title{
accum.set.table.property: Set a property on a table.
}
\description{
  Sets a property on a table in the Accumulo instance.
}
\usage{
accum.set.table.property(tablename, name, value, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{tablename}{
The name of the table to set the property on.
}
  \item{name}{
The name of the property to set.
}
  \item{value}{
The value of the property to set.
}

  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the property is successfully set, error otherwise.
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
  \dontrun{accum.set.table.property("test_proxy","table.failures.ignore","false")}
}
