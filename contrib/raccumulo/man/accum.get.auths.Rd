\name{accum.get.auths}
\alias{accum.get.auths}
\title{
accum.get.auths: List user's authorizations
}
\description{
  Provides a character vector of the user's authorizations in the Accumulo instance.
}
\usage{
accum.get.auths(username, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The user to get authorizations for.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A character vector of authorizations.
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
  \dontrun{accum.get.auths("proxy-user")}
}
