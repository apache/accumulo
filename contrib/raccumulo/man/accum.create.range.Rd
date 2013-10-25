\name{accum.create.range}
\alias{accum.create.range}
\title{
accum.create.range: Create a range.
}
\description{
  Creates a range data structure for use with scanner options.
}
\usage{
accum.create.range(start,startInclusive=TRUE,stop,stopInclusive=TRUE)
}
\arguments{
  \item{start}{
The row key to begin scanning at (as returned from accum.create.key).
}
  \item{startInclusive}{
Include rows with the start key.
}
  \item{stop}{
The row key to stop scanning at (as returned from accum.create.key).
}
  \item{stopInclusive}{
Include rows with the start key.
}

}
\value{
   A named list containing the range structure.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
  \code{\link{accum.create.scan.options}}
  \code{\link{accum.create.batch.scan.options}}
  \code{\link{accum.create.key}}
}
\examples{
  library(raccumulo)
  key <- accum.create.key("F789", "fam1", "qual1", "U&USA", 0)
  range <- accum.create.range(key,TRUE)
}
