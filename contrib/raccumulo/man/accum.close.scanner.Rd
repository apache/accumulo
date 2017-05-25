\name{accum.close.scanner}
\alias{accum.close.scanner}
\title{
accum.close.scanner: Close a scanner.
}
\description{
  Closes a scanner and deallocates resources.
}
\usage{
accum.close.scanner(scanner, hbc = accum.defaults("hbc"))
}
\arguments{
  \item{scanner}{
The scanner to close (returned from accum.create.scanner() or accum.create.batch.scanner()).
}
  \item{hbc}{
The pointer to the AccumuloClient (returned from accum.init()).
}
}
\value{
  TRUE if the scanner was successfully closed, error otherwise.
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
  \dontrun{accum.close.scanner(scanner)}
}
