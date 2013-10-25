\name{accum.init}
\alias{accum.init}
\title{
accum.init: Initialize a connection.
}
\description{
Initializes the connection to the Accumulo Thrift server.
}
\usage{
accum.init(host = "127.0.0.1", port = 42424)
}
\arguments{
  \item{host}{
The hostname or IP address of the Accumulo Thrift server.  
}
  \item{port}{
The port number the Accumulo Thrift server listens on.
}
}
\value{
An external pointer to a AccumuloClient object.  This is also stored in accum.defaults as 'hbc'.
}
\author{
Philip A Grim II (pgrim@data-tactics.com)
}
\examples{
  library(raccumulo)
  \dontrun{accum.init('localhost',42424)}
}