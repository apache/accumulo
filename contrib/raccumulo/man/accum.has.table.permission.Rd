\name{accum.has.table.permission}
\alias{accum.has.table.permission}
\title{
accum.has.table.permission: Check whether a user has a table permission.
}
\description{
  Checks whether a user has been granted a table permission in the Accumulo instance.
}
\usage{
accum.has.table.permission(username,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{username}{
The name of the user to check a table permission for.
}
  \item{permission}{
The permission to check. One of "READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", or "DROP_TABLE"
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
  \dontrun{accum.has.table.permission("proxy-user","CREATE_TABLE")}
}
