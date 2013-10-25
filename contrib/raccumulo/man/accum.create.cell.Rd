\name{accum.create.cell}
\alias{accum.create.cell}
\title{
accum.create.cell: Create a cell.
}
\description{
  Creates a cell data structure suitable for passing to update functions.
}
\usage{
accum.create.cell(row,family,qualifier="",visibility="",timestamp=0,delete=FALSE,value="")
}
\arguments{
  \item{row}{
The row key of the cell (required).
}
  \item{family}{
The column family of the cell (required).
}
  \item{qualifier}{
The column qualifier of the cell.
}
  \item{visibility}{
The visibility string of the cell.
}
  \item{timestamp}{
The timestamp of the cell.
}
  \item{delete}{
Indicates whether or not this is a delete cell.
}
  \item{value}{
The value to assign to the cell.
}
 
}
\value{
   A named list containing the cell values.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.update}}\cr
\code{\link{accum.update.and.flush}}\cr
}
\examples{
  library(raccumulo)
  updates <- list()
  updates[[1]] <- accum.create.cell("F789", "fam1", "qual1", "U&USA", 0, FALSE, "four")
  updates[[2]] <- accum.create.cell("F789", "fam1", "qual2", "U&USA", 0, FALSE, "five")
  updates[[3]] <- accum.create.cell("F789", "fam2", "qual3", "U&USA", 0, FALSE, "six")
  updates[[4]] <- accum.create.cell("M012", "fam1", "qual1", "U&USA", 0, FALSE, "FOUR")
  updates[[5]] <- accum.create.cell("M012", "fam1", "qual2", "U&USA", 0, FALSE, "FIVE")
  updates[[6]] <- accum.create.cell("M012", "fam2", "qual3", "U&USA", 0, FALSE, "SIX")
}
