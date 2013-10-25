\name{accum.set.locality.groups}
\alias{accum.set.locality.groups}
\title{
accum.set.locality.groups: Add locality groups to a table.
}
\description{
  Adds locality groups to a table in the Accumulo instance.
}
\usage{
accum.set.locality.groups(table, groups, login=accum.defaults("login"), hbc = accum.defaults("hbc"))
}
\arguments{
  \item{table}{
The name of the table add locality groups to.
}
  \item{groups}{
A list of locality groups.
}
  \item{login}{
The pointer to the AccumuloLogin (returned from accum.login()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   TRUE if the locality groups are successfully set, error otherwise.
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
  groups <- list()
  groups$group_1 <- c("fam1","fam3")
  groups$group_2 <- c("fam2","fam4")
  \dontrun{accum.set.locality.groups("test_proxy",groups)}
}
