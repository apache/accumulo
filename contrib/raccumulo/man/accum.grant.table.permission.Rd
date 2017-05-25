\name{accum.grant.table.permission}
\alias{accum.grant.table.permission}
\title{
accum.grant.table.permission: Grant a table permission.
}
\description{
  Grants a table permission to a user in the Accumulo instance.
}
\usage{
accum.grant.table.permission(username,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{username}{
The name of the user to grant a table permission to.
}
  \item{permission}{
The permission to grant. One of "READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
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
   TRUE if the permission was successfully granted, error otherwise.
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
  \dontrun{accum.grant.table.permission("proxy-user","CREATE_TABLE")}
}
