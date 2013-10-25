\name{accum.next.entry}
\alias{accum.next.entry}
\title{
accum.next.entry: Get the next entry from a scanner.
}
\description{
  Gets the next entry from a scanner.
}
\usage{
accum.next.entry(scanner, hbc = accum.defaults("hbc"))
}
\arguments{
  \item{scanner}{
The scanner to fetch an entry from (returned from accum.create.scanner() or accum.create.batch.scanner()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
   A list containing the next entry.
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
             entry <- accum.next.entry(scanner)
           }
}
