\name{accum.set.property}
\alias{accum.set.property}
\title{
accum.set.property: Set a property on the instance.
}
\description{
  Sets a property on the Accumulo instance.
}
\usage{
accum.set.property(name, value, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
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
  \dontrun{accum.set.property("table.failures.ignore","false")}
}
