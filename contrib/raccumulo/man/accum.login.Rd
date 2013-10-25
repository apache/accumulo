\name{accum.login}
\alias{accum.login}
\title{
accum.login: Log into Accumulo.
}
\description{
  Logs into Accumulo using the provided credentials.
}
\usage{
accum.login(user, password, hbc = accum.defaults("hbc"))
}
\arguments{
  \item{user}{
The Accumulo username.
}
  \item{password}{
The user's Accumulo password.
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   An external pointer to a Accumulo login token.  Also saved in accum.defaults as 'login'.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
}
