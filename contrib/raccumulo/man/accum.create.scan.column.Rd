\name{accum.create.scan.column}
\alias{accum.create.scan.column}
\title{
accum.create.scan.column: Create a scan.column.
}
\description{
  Creates a scan column data structure suitable for passing to accum.create.scan.options or accum.create.batch.scan.options.
}
\usage{
accum.create.scan.column(colFamily="",colQualifier="")
}
\arguments{
  \item{family}{
The column family (required).
}
  \item{qualifier}{
The column qualifier.
}
}
\value{
   A named list containing the scan column structure.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
  \code{\link{accum.create.scan.options}}\cr
  \code{\link{accum.create.batch.scan.options}}\cr
}
\examples{
  library(raccumulo)
  scan.columns <- list()
  scan.columns[[1]] <- accum.create.scan.column('fam1','qual1')
  scan.columns[[2]] <- accum.create.scan.column('fam1','qual2')

}
