\name{accum.create.writer.options}
\alias{accum.create.writer.options}
\title{
accum.create.writer.options: Create writer options.
}
\description{
  Creates a named list representing writer options to be passed to accum.create.writer.
}
\usage{
accum.create.writer.options(maxMemory=2048000,latencyMs=5000,threads=4)
}
\arguments{
  \item{maxMemory}{
The size in bytes of the writer's buffer (default 2M).
}
  \item{latencyMs}{
The latency in milliseconds (default 5000).
}
  \item{threads}{
The number of writer threads (default 4).
} 
}
\value{
   A named list containing the options.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}

\seealso{
\code{\link{accum.create.writer}}\cr
}
\examples{
  library(raccumulo)
  opts <- accum.create.writer.options(4096000,3000,6)
}
