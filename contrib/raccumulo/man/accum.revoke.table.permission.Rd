\name{accum.revoke.table.permission}
\alias{accum.revoke.table.permission}
\title{
accum.revoke.table.permission: Revoke a table permission.
}
\description{
  Revokes a table permission from a user in the Accumulo instance.
}
\usage{
accum.revoke.table.permission(username,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{username}{
The name of the user to revoke a table permission from.
}
  \item{permission}{
The permission to revoke. One of "READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
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
   TRUE if the permission was successfully revoked, error otherwise.
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
  \dontrun{accum.revoke.table.permission("proxy-user","CREATE_TABLE")}
}
