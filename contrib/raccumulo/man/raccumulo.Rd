\name{raccumulo}
\alias{raccumulo}
\docType{package}
\title{raccumulo: Interact with Accumulo from R} 
\description{
   The functions in this package allow one to explore Accumulo from R.
   Functionality provided is similar to the functionality of the Accumulo shell.
}
\seealso{
Connection:\cr
  \code{\link{accum.init}}\cr
  \code{\link{accum.login}}\cr
  
Table Operations:\cr
  \code{\link{accum.table.exists}}\cr
  \code{\link{accum.list.tables}}\cr
  \code{\link{accum.create.table}}\cr
  \code{\link{accum.delete.table}}\cr
  \code{\link{accum.offline.table}}\cr
  \code{\link{accum.online.table}}\cr
  \code{\link{accum.rename.table}}\cr
  \code{\link{accum.add.splits}}\cr
  \code{\link{accum.list.splits}}\cr
  \code{\link{accum.clear.locator.cache}}\cr
  \code{\link{accum.flush.table}}\cr
  \code{\link{accum.get.locality.groups}}\cr
  \code{\link{accum.set.locality.groups}}\cr
  \code{\link{accum.get.table.properties}}\cr
  \code{\link{accum.set.table.property}}\cr
  
User Operations:\cr
  \code{\link{accum.create.user}}\cr
  \code{\link{accum.authenticate.user}}\cr
  \code{\link{accum.set.auths}}\cr
  \code{\link{accum.get.auths}}\cr
  \code{\link{accum.set.password}}\cr
  \code{\link{accum.grant.system.permission}}\cr
  \code{\link{accum.has.system.permission}}\cr
  \code{\link{accum.revoke.system.permission}}\cr
  \code{\link{accum.grant.table.permission}}\cr
  \code{\link{accum.has.table.permission}}\cr
  \code{\link{accum.revoke.table.permission}}\cr
  \code{\link{accum.delete.user}}\cr
  \code{\link{accum.list.users}}\cr
  

Instance Operations:\cr
  \code{\link{accum.get.active.scans}}\cr
  \code{\link{accum.list.tablet.servers}}\cr
  \code{\link{accum.set.property}}\cr
  \code{\link{accum.remove.property}}\cr

CRUD Operations:\cr
  \code{\link{accum.create.batch.scanner}}\cr
  \code{\link{accum.create.scanner}}\cr
  \code{\link{accum.has.next}}\cr
  \code{\link{accum.next.entry}}\cr
  \code{\link{accum.next.k}}\cr
  \code{\link{accum.close.scanner}}\cr
  \code{\link{accum.scan}}\cr
  \code{\link{accum.create.writer}}\cr
  \code{\link{accum.update}}\cr
  \code{\link{accum.flush.writer}}\cr
  \code{\link{accum.close.writer}}\cr
  
Helper Functions:
  \code{\link{accum.create.cell}}\cr
  \code{\link{accum.create.writer.options}}\cr
  \code{\link{accum.create.scan.options}}\cr
  \code{\link{accum.create.batch.scan.options}}\cr
  \code{\link{accum.create.key}}\cr
  \code{\link{accum.create.range}}\cr
  \code{\link{accum.create.scan.column}}\cr
  
}
