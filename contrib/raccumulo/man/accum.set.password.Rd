\name{accum.set.password}
\alias{accum.set.password}
\title{
accum.set.password: Set a user's password.
}
\description{
  Sets a user's password in the Accumulo instance.
}
\usage{
accum.set.password(username, password, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The name of the user to set the password for.
}
  \item{password}{
The new password to set.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user's password was successfully set, error otherwise.
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
  \dontrun{accum.set.password("proxy-user","MyPassword")}
}
