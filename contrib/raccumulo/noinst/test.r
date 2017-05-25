## Copyright 2013 Data Tactics Corporation
##   
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, 
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

## Initialization
library(raccumulo)
accum.init("localhost",42424)
accum.login("user","password")

## Table operations
accum.list.tables()
accum.create.table("proxy_test",TRUE,"MILLIS")
accum.create.table("delete_test",FALSE,"LOGICAL")
accum.delete.table("delete_test")
accum.offline.table("proxy_test")
accum.online.table("proxy_test")
accum.rename.table("proxy_test", "test_proxy")
accum.table.exists("test_proxy")
accum.clone.table("test_proxy","test_clone");
accum.set.table.property("test_proxy","tserver.wal.replication","3")
accum.get.table.properties("test_proxy")
accum.remove.table.property("test_proxy","tserver.wal.replication")
accum.add.splits("test_proxy",c("C","G","M","S"))
accum.list.splits("test_proxy")
accum.merge.tablets("test_proxy","F","N")
##constraint <- accum.add.constraint("test_proxy","a.constraint.class")
accum.list.constraints("test_proxy")
##accum.remove.constraint("test_proxy",constraint)
#accum.check.iterator.conflicts("test_proxy","anIteratorName","an.iterator.class",1)
#accum.attach.iterator("test_proxy","anIteratorName","an.iterator.class",1)
accum.list.iterators("test_proxy")
accum.get.iterator.setting("test_proxy","vers","SCAN")
#accum.remove.iterator("test_proxy","anIteratorName")
groups <- list()
groups$group_1 <- c("fam1","fam3")
groups$group_2 <- c("fam2","fam4")
accum.set.locality.groups("test_proxy",groups)
accum.get.locality.groups("test_proxy")
accum.flush.table("test_proxy","A","Z",TRUE)
accum.clear.locator.cache("test_proxy")
accum.table.id.map()
accum.disk.usage("test_proxy")
#accum.test.table.class.load("test_proxy","a.class.name","aType")

## User operations
accum.create.user("proxy_user","password")
accum.set.password("proxy_user","drowssap")
accum.list.users()
accum.set.auths("proxy_user","U","USA")
accum.get.auths("proxy_user")
accum.grant.system.permission("proxy_user","CREATE_TABLE")
accum.grant.table.permission("proxy_user","test_proxy", "READ")
accum.grant.table.permission("proxy_user","test_proxy", "WRITE")
accum.has.system.permission("proxy_user","CREATE_TABLE")
accum.has.table.permission("proxy_user","test_proxy","WRITE")
accum.revoke.system.permission("proxy_user","CREATE_TABLE")
accum.revoke.table.permission("proxy_user","test_proxy", "WRITE")
accum.authenticate.user("proxy_user","drowssap")
accum.delete.user("proxy_user")

## Instance operations
accum.get.system.config()
accum.get.site,config()
accum.set.property("tserver.wal.replication","3")
accum.remove.property("tserver.wal.replication")
accum.list.tablet.servers()
accum.ping.tablet.server("127.0.0.1:9997")
#accum.test.class.load("a.class.name","aType")
accum.get.active.scans("127.0.0.1:9997")
accum.get.active.compactions("127.0.0.1:9997")

## CRUD operations
updates <- list()
updates[[1]] <- accum.create.cell("A123", "fam1", "qual1", "U&USA", 0, FALSE, "one")
updates[[2]] <- accum.create.cell("A123", "fam1", "qual2", "U&USA", 0, FALSE, "two")
updates[[3]] <- accum.create.cell("A123", "fam2", "qual3", "U&USA", 0, FALSE, "three")
updates[[4]] <- accum.create.cell("C456", "fam1", "qual1", "U&USA", 0, FALSE, "ONE")
updates[[5]] <- accum.create.cell("C456", "fam1", "qual2", "U&USA", 0, FALSE, "TWO")
updates[[6]] <- accum.create.cell("C456", "fam2", "qual3", "U&USA", 0, FALSE, "THREE")

accum.update.and.flush("test_proxy",updates)

updates <- list()
updates[[1]] <- accum.create.cell("F789", "fam1", "qual1", "U&USA", 0, FALSE, "four")
updates[[2]] <- accum.create.cell("F789", "fam1", "qual2", "U&USA", 0, FALSE, "five")
updates[[3]] <- accum.create.cell("F789", "fam2", "qual3", "U&USA", 0, FALSE, "six")
updates[[4]] <- accum.create.cell("M012", "fam1", "qual1", "U&USA", 0, FALSE, "FOUR")
updates[[5]] <- accum.create.cell("M012", "fam1", "qual2", "U&USA", 0, FALSE, "FIVE")
updates[[6]] <- accum.create.cell("M012", "fam2", "qual3", "U&USA", 0, FALSE, "SIX")

writer <- accum.create.writer("test_proxy")
accum.update(writer,updates)
accum.flush.writer(writer)
accum.close.writer(writer)

scanner <- accum.create.scanner("test_proxy");
while (accum.has.next(scanner)){
  print(accum.next.entry(scanner))
}
accum.close.scanner(scanner);

scanner <- accum.create.batch.scanner("test_proxy")
while(accum.has.next(scanner)){
  rw <- accum.next.k(scanner,3)
  i <- 1
  while (i <= length(rw$results)){
    key <- paste(rw$results[[i]]$row," ",rw$results[[i]]$colFamily,":",rw$results[[i]]$colQualifier," [",rw$results[[i]]$colVisibility,"] ",sep="")
    value <- rw$results[[i]]$value
    print(paste(key,value))
    i <- i + 1
  }
}
accum.close.scanner(scanner)

accum.delete.rows("test_proxy","M","Z")
accum.scan("test_proxy")

accum.get.max.row("test_proxy",c("U","USA"),"A",TRUE,"Z",TRUE)

## Import/Export operations
accum.offline.table("test_proxy")
accum.export.table("test_proxy","/tmp/test_proxy")
accum.import.table("test_proxy","/tmp/test_proxy")
accum.import.directory("test_proxy","/tmp/test_proxy","/tmp/fail",FALSE)

