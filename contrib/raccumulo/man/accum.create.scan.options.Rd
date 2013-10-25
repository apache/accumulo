\name{accum.create.scan.options}
\alias{accum.create.scan.options}
\title{
accum.create.scan.options: Create scan options.
}
\description{
  Creates a named list representing scanner options to be passed to accum.create.scanner().
}
\usage{
accum.create.scan.options(authorizations=list(),range=list(),columns=list(),bufferSize=2048000)
}
\arguments{
  \item{authorizations}{
A list of authorization tokens.
}
  \item{range}{
The range of keys to scan. Use accum.create.range to build a range. Empty range scans all rows.
}
  \item{columns}{
A list of column family/qualifiers to return (empty returns all columns). Use accum.create.scan.column to build columns.
} 
  \item{bufferSize}{
Size in bytes for the scan buffer.
}
}
\value{
   A named list containing the options.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.create.scanner}}\cr
\code{\link{accum.create.range}}\cr
\code{\link{accum.create.scan.column}}\cr
}
\examples{
  library(raccumulo)
  authorizations <- list()
  authorizations[[1]] <- c("U");
  opts <- accum.create.scan.options(authorizations)
}
