\name{accum.create.key}
\alias{accum.create.key}
\title{
accum.create.key: Create a key.
}
\description{
  Creates a key data structure.
}
\usage{
accum.create.key(row,colFamily,colQualifier="",colVisibility="",timestamp=0)
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

}
\value{
   A named list containing the key structure.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{

}
\examples{
  library(raccumulo)
  key <- accum.create.key("F789", "fam1", "qual1", "U&USA", 0)
}
