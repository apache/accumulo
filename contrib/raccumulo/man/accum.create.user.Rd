\name{accum.create.user}
\alias{accum.create.user}
\title{
accum.create.user: Create a user.
}
\description{
  Creates a user in the Accumulo instance.
}
\usage{
accum.create.user(username, password, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The name of the user to create.
}
  \item{password}{
The password to set.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user was successfully created, error otherwise.
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
  \dontrun{accum.create.user("proxy-user","mypassword")}
}
