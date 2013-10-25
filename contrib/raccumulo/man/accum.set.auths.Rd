\name{accum.set.auths}
\alias{accum.set.auths}
\title{
accum.set.auths: Set a user's authorizations.
}
\description{
  Sets a user's authorizations in the Accumulo instance.
}
\usage{
accum.set.auths(username, ..., login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The name of the user to set authorizations for.
}
  \item{...}{
Comma separated strings representing authorizations to set.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user's authorizations are successfully set, error otherwise.
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
  \dontrun{accum.set.auths("proxy-user","U","USA")}
}
