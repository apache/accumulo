\name{accum.create.batch.scan.options}
\alias{accum.create.batch.scan.options}
\title{
accum.create.batch.scan.options: Create batch scan options.
}
\description{
  Creates a named list representing batch scanner options to be passed to accum.create.batch.scanner().
}
\usage{
accum.create.batch.scan.options(authorizations=list(),ranges=list(),columns=list(),threads=4)
}
\arguments{
  \item{authorizations}{
A list of authorization tokens.
}
  \item{ranges}{
A list of ranges of keys to scan. Use accum.create.range to build a range. Empty range scans all rows.
}
  \item{columns}{
A list of column family/qualifiers to return (empty returns all columns). Use accum.create.scan.column to build columns.
} 
  \item{threads}{
The number of threads to scan with.
}
}
\value{
   A named list containing the options.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.create.batch.scanner}}\cr
\code{\link{accum.create.range}}\cr
\code{\link{accum.create.scan.column}}\cr
}
\examples{
  library(raccumulo)
  authorizations <- list()
  authorizations[[1]] <- c("U");
  opts <- accum.create.batch.scan.options(authorizations)
}
