\name{accum.has.system.permission}
\alias{accum.has.system.permission}
\title{
accum.has.system.permission: Check whether a user has a system permission.
}
\description{
  Checks whether a user has been granted a system permission in the Accumulo instance.
}
\usage{
accum.has.system.permission(username,permission=c("GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", "SYSTEM"),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{username}{
The name of the user to check a system permission for.
}
  \item{permission}{
The permission to check. One of "GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", or "SYSTEM"
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the user has the permission, FALSE otherwise.
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
  \dontrun{accum.has.system.permission("proxy-user","CREATE_TABLE")}
}
