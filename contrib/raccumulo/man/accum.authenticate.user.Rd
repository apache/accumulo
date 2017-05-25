\name{accum.authenticate.user}
\alias{accum.authenticate.user}
\title{
accum.authenticate.user: Authenticate a user.
}
\description{
  Authenticates a user's login in the Accumulo instance.
}
\usage{
accum.authenticate.user(username, password, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The name of the user to authenticate.
}
  \item{password}{
The user's password.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user is successfully authenticated, FALSE otherwise.
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
  \dontrun{accum.authenticate.user("proxy-user","mypassword")}
}
