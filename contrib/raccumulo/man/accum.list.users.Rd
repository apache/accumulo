\name{accum.list.users}
\alias{accum.list.users}
\title{
accum.list.users: List user names.
}
\description{
  Provides a character vector of the user names in the Accumulo instance.
}
\usage{
accum.list.users(login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A character vector of user names.
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
  \dontrun{accum.list.users()}
}
