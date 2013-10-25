\name{accum.has.next}
\alias{accum.has.next}
\title{
accum.has.next: Check for more results on a scanner.
}
\description{
  Checks for another entry on a scanner.
}
\usage{
accum.has.next(scanner, hbc = accum.defaults("hbc"))
}
\arguments{
  \item{scanner}{
The scanner to check for more results (returned from accum.create.scanner() or accum.create.batch.scanner()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A pointer to a scanner.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}\cr
\code{\link{accum.create.scanner}}\cr
\code{\link{accum.create.batch.scanner}}\cr
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{scanner <- accum.create.scanner("proxy-test")}
  \dontrun{accum.has.next(scanner)}
}
