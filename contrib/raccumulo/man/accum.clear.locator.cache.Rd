\name{accum.clear.locator.cache}
\alias{accum.clear.locator.cache}
\title{
accum.clear.locator.cache: Clear table locator cache.
}
\description{
  Clears the locator cache for a table in the Accumulo instance.
}
\usage{
accum.clear.locator.cache(table,login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The table to clear the locator cache for.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if successful, error otherwise.
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
  \dontrun{accum.clear.locator.cache("test-proxy")}
}
