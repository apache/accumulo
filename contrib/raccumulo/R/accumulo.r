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

accum.defaults <- function(arg){
  if(missing(arg)){
    as.list(.accumEnv)
  } else raccumulo:::.accumEnv[[arg]]
}

accum.init <- function(host='127.0.0.1', port=42424){
  ## initializes an Accumulo thrift connection
  ## host     = the hostname of the thrift server
  ## port     = the port on which the thrift server is listening
  
  ## return: an object of class "accumulo.client"
  y <- .Call("initialize",host,as.integer(port), PACKAGE="raccumulo")
  class(y) <- "accumulo.client"
  reg.finalizer(y,function(r){
    .Call("deleteclient",r,PACKAGE="raccumulo")
  },TRUE)
  assign('hbc',y,envir=raccumulo:::.accumEnv)
  y
}

accum.login <- function(user,password,hbc=accum.defaults('hbc')){
  ## Provides a login to the proxied accumulo instance
  ## user = The username to log in as
  ## password = The password for the user
  ## return: A login object
  y <- .Call("accum_login",hbc,as.character(user),as.character(password),PACKAGE="raccumulo")
  class(y) <- "accumulo.login"
  reg.finalizer(y,function(r){
    .Call("deletestrptr",r,PACKAGE="raccumulo")
  },TRUE)
  assign('login',y,envir=raccumulo:::.accumEnv)
  y
}

accum.list.tables <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Provides a character vector of tables
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: a data frame of tables
  y <- .Call("accum_get_tables",login,hbc,PACKAGE="raccumulo")
  y
}

accum.create.table <- function(tablename,versioningIter,timeType=c("LOGICAL","MILLIS"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){ 
  ## Creates a new table
  ## tablename = The name of the new table to create
  ## versioningIter = TRUE if a versioning iterator is desired
  ## timeType = the type of time to use (LOGICAL or MILLIS)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was created, error otherwise
  timeType<-match.arg(timeType)
  .Call("accum_create_table",login,hbc,as.character(tablename),as.logical(versioningIter),as.character(timeType),PACKAGE="raccumulo")
  TRUE
}

accum.clone.table <- function(tablename,newname,flush=TRUE,properties=list(),exclude=list(),login=accum.defaults('login'), hbc=accum.defaults('hbc')){ 
  ## Creates clone of a table
  ## tablename = The name of the table to clone
  ## newname = The name of the clone
  ## flush = Indicates whether or not to flush the table before cloning
  ## properties = A named list of properties to set on the clone
  ## exclude = A list of property names to exclude from the clone
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was created, error otherwise
  .Call("accum_clone_table",login,hbc,as.character(tablename),as.character(newname),as.logical(flush),properties,exclude,PACKAGE="raccumulo")
  TRUE
}

accum.delete.table <- function(tablename,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Deletes a table
  ## tablename = The name of the table to delete
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was deleted, error otherwise
  .Call("accum_delete_table",login,hbc,as.character(tablename))
  TRUE
} 

accum.online.table <- function(tablename,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Sets a table to online
  ## tablename = The name of the table
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was onlined, error otherwise
  .Call("accum_online_table",login,hbc,as.character(tablename),PACKAGE="raccumulo")
  TRUE
}

accum.offline.table <- function(tablename,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Sets a table to offline
  ## tablename = The name of the table
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was offlined, error otherwise
  .Call("accum_offline_table",login,hbc,as.character(tablename),PACKAGE="raccumulo")
  TRUE
}

accum.rename.table <- function(oldname,newname,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Changes the name of a table
  ## oldname = The name of the table
  ## newname = The new name of the table
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table was renamed, error otherwise
  .Call("accum_rename_table",login,hbc,as.character(oldname),as.character(newname),PACKAGE="raccumulo")
  TRUE
}

accum.table.exists <- function(tablename,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Tests the existence of a table
  ## tablename = The name of the table to check
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the table exists, false otherwise
  y <- .Call("accum_table_exists",login,hbc,as.character(tablename),PACKAGE="raccumulo")
  y
}

accum.create.user <- function(username,password,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Creates a user in the accumulo instance
  ## username = The name of the user to create
  ## password = The password to set for the user
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the user was created, error otherwise
  y <- .Call("accum_create_user",login,hbc,as.character(username),as.character(password),PACKAGE="raccumulo")
  TRUE
}

accum.delete.user <- function(username,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Deletes a user in the accumulo instance
  ## username = The name of the user to delete
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the user was deleted, error otherwise
  y <- .Call("accum_delete_user",login,hbc,as.character(username))
  TRUE
}

accum.set.password <- function(username,password,login=accum.defaults('login'),hbc=accum.defaults("hbc")){
  ## Sets a user's password in the accumulo instance
  ## username = The name of the user to change
  ## password = The password to set for the user
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the password was changed, error otherwise
  y <- .Call("accum_set_password",login,hbc,as.character(username),as.character(password),PACKAGE="raccumulo")
  TRUE
}

accum.list.users <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Provides a character vector of usernames
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: a data frame of usernames
  y <- .Call("accum_list_users",login,hbc,PACKAGE="raccumulo")
  y
}

accum.get.auths <- function(username,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Provides a character vector of authorization strings
  ## username = The name of the user to get the authorizations for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: a data frame of authorizations
  y <- .Call("accum_get_auths",login,hbc,as.character(username),PACKAGE="raccumulo")
  y
}

accum.set.auths <- function(username,...,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Sets the list of authorizations for a user
  ## username = The name of the user to set the authorizations for
  ## ... = list of authorizations
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if auths are successfully set, error otherwise
  auths <- list(...)
  .Call("accum_set_auths",login,hbc,as.character(username),auths,PACKAGE="raccumulo")
  TRUE
}

accum.grant.system.permission <- function(username,permission=c("GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", "SYSTEM"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Grants the specified permission to the specified user
  ## username = The name of the user to grant the permission to
  ## permission = The permission to grant (one of GRANT, CREATE_TABLE, DROP_TABLE, ALTER_TABLE, CREATE_USER, DROP_USER, ALTER_USER, SYSTEM)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if permission is successfully granted, error otherwise
  permission<-match.arg(permission)
  .Call("accum_grant_system_permission",login,hbc,as.character(username),as.character(permission),PACKAGE="raccumulo")
  TRUE
}

accum.grant.table.permission <- function(username,tablename,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Grants the specified permission on the specified table to the specified user
  ## username = The name of the user to grant the permission to
  ## tablename = The name of the table to grant the permission on
  ## permission = The permission to grant (one of READ, WRITE, BULK_IMPORT, ALTER_TABLE, GRANT, DROP_TABLE)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if permission is successfully granted, error otherwise
  permission<-match.arg(permission)
  .Call("accum_grant_table_permission",login,hbc,as.character(username),as.character(tablename),as.character(permission),PACKAGE="raccumulo")
  TRUE
}

accum.has.system.permission <- function(username,permission=c("GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", "SYSTEM"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Queries to see if the specified user has the specified system permission
  ## username = The name of the user to check
  ## permission = The permission to check for (one of GRANT, CREATE_TABLE, DROP_TABLE, ALTER_TABLE, CREATE_USER, DROP_USER, ALTER_USER, SYSTEM)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the user has the specified permission, FALSE otherwise
  permission<-match.arg(permission)
  y <- .Call("accum_has_system_permission",login,hbc,as.character(username),as.character(permission),PACKAGE="raccumulo")
  y
}

accum.has.table.permission <- function(username,tablename,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Queries to see if the specified user has the specified table permission
  ## username = The name of the user to check
  ## tablename = The name of the table to check
  ## permission = The permission to check for (one of READ, WRITE, BULK_IMPORT, ALTER_TABLE, GRANT, DROP_TABLE)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the user has the specified permission, FALSE otherwise
  permission<-match.arg(permission)
  y <- .Call("accum_has_table_permission",login,hbc,as.character(username),as.character(tablename),as.character(permission),PACKAGE="raccumulo")
  y
}

accum.revoke.system.permission <- function(username,permission=c("GRANT", "CREATE_TABLE", "DROP_TABLE", "ALTER_TABLE", 
 "CREATE_USER", "DROP_USER", "ALTER_USER", "SYSTEM"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Revoke the specified permission from the specified user
  ## username = The name of the user to revoke the permission from
  ## permission = The permission to revoke (one of GRANT, CREATE_TABLE, DROP_TABLE, ALTER_TABLE, CREATE_USER, DROP_USER, ALTER_USER, SYSTEM)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if permission is successfully revoked, error otherwise
  permission<-match.arg(permission)
  .Call("accum_revoke_system_permission",login,hbc,as.character(username),as.character(permission),PACKAGE="raccumulo")
  TRUE
}

accum.revoke.table.permission <- function(username,tablename,permission=c("READ", "WRITE", "BULK_IMPORT", "ALTER_TABLE", 
 "GRANT", "DROP_TABLE"),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Revokes the specified permission on the specified table from the specified user
  ## username = The name of the user to revoke the permission from
  ## tablename = The name of the table to revoke the permission on
  ## permission = The permission to revoke (one of READ, WRITE, BULK_IMPORT, ALTER_TABLE, GRANT, DROP_TABLE)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if permission is successfully revoked, error otherwise
  permission<-match.arg(permission)
  .Call("accum_revoke_table_permission",login,hbc,as.character(username),as.character(tablename),as.character(permission),PACKAGE="raccumulo")
  TRUE
}

accum.authenticate.user <- function(username,password,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Checks a user's credentials
  ## username = The name of the user to check
  ## password = The password of the user to check
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the username/password combo are valid, FALSE otherwise
  y <- .Call("accum_authenticate_user",login,hbc,as.character(username),as.character(password),PACKAGE="raccumulo")
  y
}

accum.set.property <- function(name,value,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Sets a property on the session
  ## name = The name of the property to set
  ## value = The value to be assigned to the property
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the property is successfully set, error otherwise
  .Call("accum_set_property",login,hbc,as.character(name),as.character(value),PACKAGE="raccumulo")
  TRUE
}

accum.remove.property <- function(name,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Removes a property from the session
  ## name = The name of the property to remove
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the property is successfully removed, error otherwise
  .Call("accum_remove_property",login,hbc,as.character(name),PACKAGE="raccumulo")
  TRUE
}

accum.set.table.property <- function(tablename,name,value,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Sets a property on the specified table
  ## tablename = The name of the table to set the property on
  ## name = The name of the property to set
  ## value = The value to be assigned to the property
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the property is successfully set, error otherwise
  .Call("accum_set_table_property",login,hbc,as.character(tablename),as.character(name),as.character(value),PACKAGE="raccumulo")
  TRUE
}

accum.remove.table.property <- function(tablename,name,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Removes a property from the specified table
  ## tablename = The name of the table to remove the property from
  ## name = The name of the property to remove
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the property is successfully removed, error otherwise
  .Call("accum_remove_table_property",login,hbc,as.character(tablename),as.character(name),PACKAGE="raccumulo")
  TRUE
}

accum.get.table.properties <- function(tablename,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the properties for a table
  ## tablename = The name of the table to get the properties of
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the properties on the table
  y <- .Call("accum_get_table_properties",login,hbc,as.character(tablename),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.list.tablet.servers <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Provides a character vector of tablet servers
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: a data frame of tablet servers
  y <- .Call("accum_get_tablet_servers",login,hbc,PACKAGE="raccumulo")
  y
}

accum.ping.tablet.server <- function(tablet_server,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Checks to see if a tablet server is alive
  ## tablet_server = The name of the tablet server to ping
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the tablet server is alive, error otherwise
  .Call("accum_ping_tablet_server",login,hbc,as.character(tablet_server),PACKAGE="raccumulo")
  TRUE
}

accum.merge.tablets <- function(table,start,end,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Merges a set of tablets.
  ## table = The table whose tablets are to be merged
  ## start = The row key to begin the merge from
  ## end = the row key to end the merge at
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the merge is successful, error otherwise
  .Call("accum_merge_tablets",login,hbc,as.character(table),as.character(start),as.character(end),PACKAGE="raccumulo")
  TRUE
}

accum.add.constraint <- function(table,constraint_class,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Adds a constraint to an Accumulo table.
  ## table = The name of the table to add the constraint to
  ## constraint_class = The class name of the constraint to add
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: An integer representing the unique id of the constraint
  y <- .Call("accum_add_constraint",login,hbc,as.character(table),as.character(constraint_class),PACKAGE="raccumulo")
  y
}

accum.remove.constraint <- function(table,constraint,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Removes a constraint from an Accumulo table.
  ## table = The name of the table to remove the constraint from
  ## constraint = The id of the constraint to remove (returned from accum.add.constraint)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the constraint is removed, error otherwise
  .Call("accum_remove_constraint",login,hbc,as.character(table),as.integer(constraint),PACKAGE="raccumulo")
  TRUE
}

accum.list.constraints <- function(tablename,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the constraints for a table
  ## tablename = The name of the table to get the constraints for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the constraints on the table
  y <- .Call("accum_list_constraints",login,hbc,as.character(tablename),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.clear.locator.cache <- function(table,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Clears the locator cache from an Accumulo table.
  ## table = The name of the table to clear the cache from
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the cache is cleared, error otherwise
  .Call("accum_clear_locator_cache",login,hbc,as.character(table),PACKAGE="raccumulo")
  TRUE
}

accum.import.table <- function(table,importDir,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Imports a table from an export contained in a directory
  ## table = The name of the table to import to
  ## importDir = The directory containing the export to load
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the import is successful, error otherwise
  .Call("accum_import_table",login,hbc,as.character(table),as.character(importDir),PACKAGE="raccumulo")
  TRUE
}

accum.import.directory <- function(table,importDir,failureDir,setTime,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Imports a table from data in a directory
  ## table = The name of the table to import to
  ## importDir = The directory containing the data to load
  ## failureDir = The directory to write failures to
  ## setTime = Indicates whether or not to set the current time as the timestamp on the import
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the import is successful, error otherwise
  .Call("accum_import_directory",login,hbc,as.character(table),as.character(importDir),
        as.character(failureDir),as.logical(setTime),PACKAGE="raccumulo")
  TRUE
}

accum.export.table <- function(table,exportDir,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Exports a table to a directory
  ## table = The name of the table to export
  ## importDir = The directory to export to
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the export is successful, error otherwise
  .Call("accum_export_table",login,hbc,as.character(table),as.character(exportDir),PACKAGE="raccumulo")
  TRUE
}

accum.get.site.config <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the site configuration from accumulo
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the site configuration
  y <- .Call("accum_get_site_configuration",login,hbc,PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.get.system.config <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the system configuration from accumulo
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the system configuration
  y <- .Call("accum_get_system_configuration",login,hbc,PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.table.id.map <- function(login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the table id map from accumulo
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the table id map
  y <- .Call("accum_table_id_map",login,hbc,PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.disk.usage <- function(tables,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the disk usage for a table or tables
  ## tables = The list of tables to get usage for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the disk usage in bytes for each table
  y <- .Call("accum_disk_usage",login,hbc,as.character(tables),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.add.splits <- function(table,splits,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Adds a list of splits to a table
  ## tables = The table to add splits to
  ## splits = A list of splits to add to the table
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the splits are successfully added, error otherwise
  y <- .Call("accum_add_splits",login,hbc,as.character(table),as.character(splits),PACKAGE="raccumulo")
  TRUE
}

accum.list.splits <- function(table,maxsplits=9999,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Lists the splits on a table
  ## table = The table to list the splits for
  ## maxsplits = The maximum length of the resulting list of splits
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list of split points for the given table
  y <- .Call("accum_list_splits",login,hbc,as.character(table),as.integer(maxsplits),PACKAGE="raccumulo")
  y
}

accum.test.class.load <- function(class,asType,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Tests the loading of a named class
  ## class = The name of the class to load
  ## asType = The name to assign to the loaded class
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the class can be loaded, FALSE otherwise
  y <- .Call("accum_test_class_load",login,hbc,as.character(class),as.character(asType),PACKAGE="raccumulo")
  y
}

accum.test.table.class.load <- function(table,class,asType,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Tests the loading of a named class on a table
  ## table = The name of the table to test loading the class on
  ## class = The name of the class to load
  ## asType = The name to assign to the loaded class
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the class can be loaded, FALSE otherwise
  y <- .Call("accum_test_table_class_load",login,hbc,as.character(table),as.character(class),as.character(asType),PACKAGE="raccumulo")
  y
}

accum.attach.iterator <- function(table,iterName,iterClass,priority,properties,scopes=c(MINC,MAJC,SCAN),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Adds an iterator to a table
  ## table = The name of the table to attach the iterator to
  ## iterName = The name of the iterator
  ## iterClass = The class name of the iterator
  ## priority = an integer representing the priority of the iterator
  ## properties = A list of name/value pairs
  ## scopes = A list of iterator scopes (one or more of MINC,MAJC,SCAN)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the iterator is attached, error otherwise
  y <- .Call("accum_attach_iterator",login,hbc,as.character(table),as.character(iterName),as.character(iterClass),
             as.integer(priority),properties,as.character(scopes),PACKAGE="raccumulo")
  TRUE
}

accum.remove.iterator <- function(table,iterName,scopes=c(MINC,MAJC,SCAN),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Removes an iterator from a table
  ## table = The name of the table to remove the iterator from
  ## iterName = The name of the iterator
  ## scopes = A list of iterator scopes (one or more of MINC,MAJC,SCAN) to remove the iterator from
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the iterator can be attached, error otherwise
  y <- .Call("accum_remove_iterator",login,hbc,as.character(table),as.character(iterName),as.character(scopes),PACKAGE="raccumulo")
  TRUE
}

accum.check.iterator.conflicts <- function(table,iterName,iterClass,priority,properties,scopes=c(MINC,MAJC,SCAN),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Checks to see if it is possible to add an iterator to a table
  ## table = The name of the table to attach the iterator to
  ## iterName = The name of the iterator
  ## iterClass = The class name of the iterator
  ## priority = an integer representing the priority of the iterator
  ## properties = A list of name/value pairs
  ## scopes = A list of iterator scopes (one or more of MINC,MAJC,SCAN)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the iterator can be attached, error otherwise
  y <- .Call("accum_check_iterator",login,hbc,as.character(table),as.character(iterName),as.character(iterClass),
             as.integer(priority),properties,as.character(scopes),PACKAGE="raccumulo")
  TRUE
}

accum.list.iterators <- function(table,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the list of iterators for a table
  ## table = The table to list iterators for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the iterators for a table
  y <- .Call("accum_list_iterators",login,hbc,as.character(table),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.get.iterator.setting <- function(table,iterName,scope,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the iterator settings for a named iterator
  ## table = The table the iterator is attached to
  ## iterName = The name of the iterator to retrieve
  ## scope = The iterator scope to retrieve the settings for (one of MINC,MAJC,SCAN)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the iterator settings
  y <- .Call("accum_get_iterator_setting",login,hbc,as.character(table),as.character(iterName),as.character(scope),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
}

accum.get.locality.groups <- function(table,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the list of locality groups for a table
  ## table = The table to list locality groups for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the locality groups for the table
  y <- .Call("accum_get_locality_groups",login,hbc,as.character(table),PACKAGE="raccumulo")
  z <- as.list(y[[2]])
  names(z) <- y[[1]]
  z
} 

accum.set.locality.groups <- function(table,groups,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the list of locality groups for a table
  ## table = The table to list locality groups for
  ## groups = The named list of locality groups to set
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the locality groups for the table
  y <- .Call("accum_set_locality_groups",login,hbc,as.character(table),groups,PACKAGE="raccumulo")
  TRUE
}

accum.flush.table <- function(table,start,end,wait,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Flushes a table
  ## table = The table to flush
  ## start = The row key to begin the flush from
  ## end = the row key to end the flush at
  ## wait = Block until the flush finishes
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if the flush is successful, error otherwise
  .Call("accum_flush_table",login,hbc,as.character(table),as.character(start),as.character(end),as.logical(wait),PACKAGE="raccumulo")
  TRUE
}

accum.get.active.scans <- function(tablet,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the list of active scans on a tablet server
  ## tablet = The tablet server to list active scans for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the active scans on the tablet server
  y <- .Call("accum_get_active_scans",login,hbc,as.character(tablet),PACKAGE="raccumulo")
  y
} 

accum.get.active.compactions <- function(tablet,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Gets the list of active compactions on a tablet server
  ## tablet = The tablet server to list active compactions for
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing the active scans on the tablet server
  y <- .Call("accum_get_active_compactions",login,hbc,as.character(tablet),PACKAGE="raccumulo")
  y
} 

accum.create.scanner <- function(table,opts=list(),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Creates a scanner for the given table
  ## table = The table to scan
  ## opts = The named list of scanner options
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A pointer to the scanner.  This should be closed using accum.close.scanner when finished
  y <- .Call("accum_create_scanner",login,hbc,as.character(table),opts,PACKAGE="raccumulo")
  y
}

accum.create.batch.scanner <- function(table,opts=list(),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Creates a batch scanner for the given table
  ## table = The table to scan
  ## opts = The named list of batch scanner options
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A pointer to the scanner.  This should be closed using accum.close.scanner when finished
  y <- .Call("accum_create_batch_scanner",login,hbc,as.character(table),opts,PACKAGE="raccumulo")
  y
}

accum.has.next <- function(scanner, hbc=accum.defaults('hbc')){
  ## Checks to see if a scanner has more results
  ## scanner = The scanner to check for results (returned from accum.create.scanner or accum.create.batch.scanner)
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if there are more results, FALSE otherwise
  y <- .Call("accum_has_next",hbc,scanner,PACKAGE="raccumulo")
  y
}

accum.next.entry <- function(scanner, hbc=accum.defaults('hbc')){
  ## Gets the next entry on a scanner
  ## scanner = The scanner to get the next entry from (returned from accum.create.scanner or accum.create.batch.scanner)
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list of results from the scanner.
  y <- .Call("accum_next_entry",hbc,scanner,PACKAGE="raccumulo")
  y
}

accum.next.k <- function(scanner,k,hbc=accum.defaults('hbc')){
  ## Gets the next k entries on a scanner
  ## scanner = The scanner to get the entries from (returned from accum.create.scanner or accum.create.batch.scanner)
  ## k = the number of entries to retrieve
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list of results from the scanner.
  y <- .Call("accum_next_k",hbc,scanner,as.integer(k),PACKAGE="raccumulo")
  y
}

accum.close.scanner <- function(scanner, hbc=accum.defaults('hbc')){
  ## Closes a scanner and recovers memory
  ## scanner = The scanner to close (returned from accum.create.scanner or accum.create.batch.scanner)
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_close_scanner",hbc,scanner,PACKAGE="raccumulo")
  TRUE
}

accum.scan <- function(table,login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Scans a table and prints output similar to the scan command in the accumulo shell
  ## table = The table to scan
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE on successful scan, error otherwise
  scanner <- accum.create.batch.scanner(as.character(table),login=login,hbc=hbc)
  while(accum.has.next(scanner)){
    rw <- accum.next.k(scanner,10)
    i <- 1
    while (i <= length(rw$results)){
      key <- paste(rw$results[[i]]$row," ",rw$results[[i]]$colFamily,":",rw$results[[i]]$colQualifier," [",rw$results[[i]]$colVisibility,"] ",sep="")
      value <- rw$results[[i]]$value
      print(paste(key,value))
      i <- i + 1
    }
  }
  accum.close.scanner(scanner)
  TRUE
}


accum.create.writer <- function(table,opts=accum.create.writer.options(),login=accum.defaults('login'), hbc=accum.defaults('hbc')){
  ## Creates a writer for the given table
  ## table = The table to write to
  ## opts = The named list of writer options
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A pointer to the writer  This should be closed using accum.close.writer when finished
  y <- .Call("accum_create_writer",login,hbc,as.character(table),opts,PACKAGE="raccumulo")
  class(y) <- "accum.writer"
  y
}

accum.flush.writer <- function(writer, hbc=accum.defaults('hbc')){
  ## Flushes updates sent to a writer
  ## writer = The writer to flush (returned from accum.create.writer)
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_flush_writer",hbc,writer,PACKAGE="raccumulo")
  TRUE
}

accum.close.writer <- function(writer, hbc=accum.defaults('hbc')){
  ## Closes a writer and recovers memory
  ## writer = The writer to close (returned from accum.create.writer)
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_close_writer",hbc,writer,PACKAGE="raccumulo")
  TRUE
}

accum.update <- function(writer,cells,hbc=accum.defaults('hbc')){
  ## Sends an update to accumulo
  ## writer = The writer to send the update on (returned from accum.create.writer)
  ## cells = A list of cell updates
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_update",hbc,writer,cells,PACKAGE="raccumulo")
  TRUE
}

accum.update.and.flush <- function(table,cells,login=accum.defaults('login'),hbc=accum.defaults('hbc')){
  ## Sends an update to accumulo and flushes.  Equivalent to calling
  ## accum.create.writer, accum.update, accum.flush, and accum.close.writer in
  ## succession.
  ## table = The table to write to
  ## cells = A list of cell updates
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_update_and_flush",login,hbc,as.character(table),cells,PACKAGE="raccumulo")
  TRUE
}

accum.delete.rows <- function(table,start,end,login=accum.defaults('login'),hbc=accum.defaults('hbc')){
  ## Deletes rows from the specified table
  ## table = The table to delete rows from
  ## start = The row key to begin deleting from
  ## end = The row key to stop deleting at
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_delete_rows",login,hbc,as.character(table),as.character(start),as.character(end),PACKAGE="raccumulo")
  TRUE
}

accum.compact.table  <- function(table,start="",end="",iterators=list(),flush=TRUE,wait=TRUE,login=accum.defaults('login'),hbc=accum.defaults('hbc')){
  ## Compacts the specified table
  ## table = The table to compact (required)
  ## start = The row key to begin compacting from
  ## end = The row key to stop compacting at
  ## iterators = A list of iterator settings to use
  ## flush = Whether or not to flush the table
  ## wait = Whether or not to wait for the compaction to finish
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_compact_table",login,hbc,as.character(table),as.character(start),as.character(end),iterators,as.logical(flush),as.logical(wait),PACKAGE="raccumulo")
  TRUE
}

accum.cancel.compaction  <- function(table,login=accum.defaults('login'),hbc=accum.defaults('hbc')){
  ## Cancels the compaction of the specified table
  ## table = The table to cancel the compaction for
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: TRUE if successful, error otherwise
  .Call("accum_cancel_compaction",login,hbc,as.character(table),PACKAGE="raccumulo")
  TRUE
}

accum.create.cell <- function(row,family,qualifier="",visibility="",timestamp=0,delete=FALSE,value="")
  ## Creates a named list representing a cell in accumulo, suitable for passing to update functions
  ## row = The row key of the cell (required)
  ## family = The column family of the cell (required)
  ## qualifier = The column qualifier of the cell
  ## visibility = The visibility string of the cell
  ## timestamp = The timestamp of the cell
  ## delete = Indicates whether or not this is a delete cell
  ## value = the value to assign to the cell
  ## return: A named list containing the cell values
{
  cell <- list()
  if (missing(row)) stop("Row must be specified")
  if (missing(family)) stop("Column family must be specified")
  cell$row <- as.character(row)
  cell$colFamily <- as.character(family)
  cell$colQualifier <- as.character(qualifier)
  cell$colVisibility <- as.character(visibility)
  cell$timestamp <- as.integer(timestamp)
  cell$delete <- as.logical(delete)
  cell$value <- as.character(value);
  cell
}
 
accum.create.writer.options <- function(maxMemory=2048000,latencyMs=5000,timeoutMs=5000,threads=4)
  ## Creates a named list representing writer options
  ## maxMemory = The size in bytes of the writer's buffer (default 2M)
  ## latencyMs = The latency in milliseconds (default 5000)
  ## timeoutMs = The timeout in milliseconds (default 5000)
  ## threads = The number of writer threads (default 4)
  ## return: A named list containing the options
{
  opts <- list()
  opts$maxMemory <- as.integer(maxMemory)
  opts$latencyMs <- as.integer(latencyMs)
  opts$timeoutMs <- as.integer(timeoutMs)
  opts$threads <- as.integer(threads)
  opts
}

accum.create.scan.options <- function(authorizations=list(),range=list(),columns=list(),iterators=list(),bufferSize=2048000)
  ## Creates a named list representing scanner options
  ## authorizations = A list of authorization tokens
  ## range = A range of rows to scan (empty range scans all rows)
  ## columns = A list of column family/qualifiers to return (empty returns all columns)
  ## iterators = A list of iterators to use for the scan 
  ## bufferSize = Size in bytes for the scan buffer
  ## return: A named list containing the options
{
  opts <- list()
  opts$authorizations <- as.character(authorizations)
  opts$range <- range
  opts$columns <- columns
  opts$iterators <- iterators
  opts$bufferSize <- as.integer(bufferSize)
  opts
}

accum.create.batch.scan.options <- function(authorizations=list(),ranges=list(),columns=list(),iterators=list(),threads=4)
{
  ## Creates a named list representing batch scanner options
  ## authorizations = A list of authorization tokens
  ## ranges = A list of ranges of rows to scan (empty range scans all rows)
  ## columns = A list of column family/qualifiers to return (empty returns all columns)
  ## iterators = A list of iterators to use for the scan 
  ## threads = The number of threads to use for the scanner
  ## return: A named list containing the options
  opts <- list()
  opts$authorizations <- as.character(authorizations)
  opts$ranges <- ranges
  opts$columns <- columns
  opts$iterators <- iterators
  opts$threads <- as.integer(threads)
  opts
}

accum.create.key <- function(row,colFamily="",colQualifier="",colVisibility="",timestamp=0)
  ## Creates a named list representing a key in accumulo
  ## row = The row key (required)
  ## colFamily = The column family (required)
  ## colQualifier = The column qualifier 
  ## colVisibility = The visibility string
  ## timestamp = The timestamp
  ## return: A named list containing the key
{
  key <- list()
  if (missing(row)) stop("Row must be specified")
  key$row <- as.character(row)
  key$colFamily <- as.character(colFamily)
  key$colQualifier <- as.character(colQualifier)
  key$colVisibility <- as.character(colVisibility)
  key$timestamp <- as.integer(timestamp)
  key
}

accum.create.range <- function(start,startInclusive=TRUE,stop,stopInclusive=TRUE)
  ## Creates a named list representing a range in accumulo
  ## start = The starting key of a range (as returned from accum.create.key)
  ## startInclusive = whether or not the start row should be included in the range
  ## stop = The ending key of a range (as returned from accum.create.key)
  ## stopInclusive = whether or not the end row should be included in the range
  ## return: A named list containing the range
{
  range <- list()
  range$start <- start
  range$startInclusive <- as.logical(startInclusive)
  range$stop <- stop
  range$stopInclusive <- as.logical(stopInclusive)
  range
}

accum.get.row.range <- function(row,hbc=accum.defaults('hbc'))
  ## Creates a named list representing a range in accumulo
  ## row = The key to create a range for
  ## return: A named list containing the range
{
  y <- .Call("accum_get_row_range",hbc,as.character(row),PACKAGE="raccumulo")
  y
}

accum.split.range.by.tablets <- function(table,range,maxSplits=999,login=accum.defaults('login'),hbc=accum.defaults('hbc'))
  ## Creates a list of named lists representing a ranges in accumulo
  ## These ranges are subsets of the specified range divided by tablet, up to the max
  ## number specified
  ## table = The table to create ranges on
  ## range = The range to split
  ## maxSplits = the maximum number of splits to create
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A list containing named lists containing the ranges
{
  y <- .Call("accum_split_range_by_tablets",login,hbc,as.character(table),range,as.integer(maxSplits),PACKAGE="raccumulo")
  y
}

accum.get.following <- function(key,part,hbc=accum.defaults('hbc'))
  ## Gets the key that follows the specified key.
  ## key = The key to get the following key for
  ## part = The partial key specifier (one of ROW, ROW_COLFAM, ROW_COLFAM_COLQUAL, ROW_COLFAM_COLQUAL_COLVIS,
  ##                                          ROW_COLFAM_COLQUAL_COLVIS_TIME, ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL)
  ## login = A pointer to a login (returned from accum.login) 
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A named list containing the following key
{
  y <- .Call("accum_get_following",hbc,key,as.character(part),PACKAGE="raccumulo")
  y
}

accum.get.max.row <- function(table,auths=list(),startRow,startInclusive=TRUE,stopRow,stopInclusive=TRUE,login=accum.defaults('login'),hbc=accum.defaults('hbc'))
  ## Gets the max row key for a table
  ## table = the table to get the max row key for
  ## auths = the list of authorizations to use for the query
  ## startRow = the row key to start the search from
  ## startInclusive = indicates whether or not to include the start row in the search
  ## stopRow = the row key to stop the search at
  ## stopInclusive = indicates whether or not to include the stop row in the search
  ## hbc = object of class "accumulo.client" (returned from accum.init)
  ## return: A character vector containing the max key
{
  y <- .Call("accum_get_max_row",login,hbc,as.character(table),as.list(auths),as.character(startRow),as.logical(startInclusive),as.character(stopRow),as.logical(stopInclusive),PACKAGE="raccumulo")
  y
}

accum.create.scan.column <- function(colFamily="",colQualifier="")
  ## Creates a named list representing scan column (for use by accum.create.scan.options)
  ## colFamily = The column family (required)
  ## colQualifier = The column qualifier 
  ## return: A named list containing the column specifier
{
  col <- list()
  if (missing(colFamily)) stop("Column family must be specified")
  col$colFamily <- as.character(colFamily)
  col$colQualifier <- as.character(colQualifier)
  col
}

accum.create.iterator.setting <- function(name,class,priority=1,properties=list())
  ## Creates a named list representing iterator settings (for use by accum.create.scan.options)
  ## name = The name of the iterator (required)
  ## class = The name of the iterator class (required)
  ## priority = The priority of the iterator
  ## properties = A named list of properties to set
  ## return: A named list containing the iterator setting
{
  iter <- list()
  if (missing(name)) stop("Iterator name must be specified")
  if (missing(class)) stop("Iterator class must be specified")
  iter$name <- as.character(name)
  iter$class <- as.character(class)
  iter$priority <- as.integer(priority)
  iter$properties <- properties
  iter

}
