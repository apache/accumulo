\name{accum.delete.user}
\alias{accum.delete.user}
\title{
accum.delete.user: Delete a user.
}
\description{
  Deletes a user in the Accumulo instance.
}
\usage{
accum.delete.user(username, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{username}{
The name of the user to delete.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user was successfully deleted, error otherwise.
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
  \dontrun{accum.delete.user("proxy-user")}
}
