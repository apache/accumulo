\name{accum.next.k}
\alias{accum.next.k}
\title{
accum.next.k: Get the next k entries from a scanner.
}
\description{
  Gets the next k entries from a scanner.
}
\usage{
accum.next.k(scanner, k, hbc = accum.defaults("hbc"))
}
\arguments{
  \item{scanner}{
The scanner to fetch entries from (returned from accum.create.scanner() or accum.create.batch.scanner()).
}
  \item{k}{
The number of entries to fetch.
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A list containing the next k entries, plus a logical indicating whether more entries exist.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.init}}\cr
\code{\link{accum.login}}\cr
\code{\link{accum.create.scanner}}\cr
\code{\link{accum.create.batch.scanner}}\cr
\code{\link{accum.has.next}}\cr
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
  \dontrun{accum.login('accumuser','mypassword')}
  \dontrun{scanner <- accum.create.scanner("proxy-test")}
  \dontrun{if (accum.has.next(scanner)) 
             entries <- accum.next.k(scanner,10)
           }
}
