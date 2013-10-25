\name{accum.grant.system.permission}
\alias{accum.grant.system.permission}
\title{
accum.grant.system.permission: Grant a system permission.
}
\description{
  Grants a system permission to a user in the Accumulo instance.
}
\usage{
accum.grant.system.permission(username,permission=c("GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", "SYSTEM"),login=accum.defaults('login'), hbc=accum.defaults('hbc'))
}
\arguments{
  \item{username}{
The name of the user to grant a system permission to.
}
  \item{permission}{
The permission to grant. One of "GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
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
  \dontrun{accum.grant.system.permission("proxy-user","CREATE_TABLE")}
}
