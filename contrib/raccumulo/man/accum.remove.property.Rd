\name{accum.remove.property}
\alias{accum.remove.property}
\title{
accum.remove.property: Remove a property from the instance.
}
\description{
  Remove a property from the Accumulo instance.
}
\usage{
accum.remove.property(name, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{name}{
The name of the property to remove.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the property is successfully removed, error otherwise.
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
  \dontrun{accum.remove.property("table.failures.ignore")}
}
