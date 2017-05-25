// Copyright 2013 Data Tactics Corporation
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <protocol/TCompactProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <iostream>
#include <unistd.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

#include "AccumuloProxy.h"
using namespace accumulo;

#include "raccumulo.h"

extern "C"
{
  /** 
   * Initializes an Accumulo thrift connection
   * 
   * Params:
   *   host = the hostname of the thrift server
   *   port = the port on which the thrift server is listening
   *
   * Returns: A pointer to an AccumuloProxyClient
  */
  SEXP initialize(SEXP host, SEXP port)
  {
    boost::shared_ptr<TTransport> socket(new TSocket(std::string(CHAR(STRING_ELT(host,0))), INTEGER(port)[0]));
    boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TCompactProtocol(transport));

    AccumuloProxyClient *client = new AccumuloProxyClient(protocol);
    SEXP result = R_NilValue;

    try
    {
      transport->open();
      PROTECT(result = R_MakeExternalPtr((void*)client, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } 
    catch (TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Cleans up the memory allocated for the AccumuloProxyClient.  
   * Registered as a finalizer by the R interface code.
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: R_NilValue
   */
  SEXP deleteclient(SEXP r)
  {
    AccumuloProxyClient *ch  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    delete ch;
    return(R_NilValue);
  }

  /**
   * Cleans up the memory allocated for various standard strings
   * Registered as a finalizer by the R interface code.
   * 
   * Params:
   *   r = the pointer to a standard string
   *
   * Returns: R_NilValue
   */
  SEXP deletestrptr(SEXP r)
  {
    std::string *ch  = static_cast<std::string*>(R_ExternalPtrAddr(r));
    delete ch;
    return(R_NilValue);
  }

  /**
   * Logs into the proxied Accumulo instance
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   u = The username
   *   p = The password
   *
   * Returns: A pointer to a std::string containing the login token
   */
  SEXP accum_login(SEXP r, SEXP u, SEXP p)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string password(CHAR(STRING_ELT(p,0)));

    map<std::string, std::string> props;
    props["password"] = password;
    
    std::string *login = new std::string();

    try{
      client->login(*login, user, props);
      PROTECT(result = R_MakeExternalPtr((void*)login, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Gets a list of tables in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A list of tables
   */
  SEXP accum_get_tables(SEXP l, SEXP r)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::set<std::string> tables;
    try
    {
      client->listTables(tables,*login);
      if (tables.size()>0)
      {
        PROTECT(result = Rf_allocVector(STRSXP,tables.size()));
        UNPROTECT(1);
        int i = 0;
	for (std::set<std::string>::iterator it = tables.begin(); it != tables.end(); ++it)
        {
          SET_STRING_ELT(result, i++, Rf_mkChar(static_cast<const char*>((*it).c_str())));
        }
      }
    } 
    catch( TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Creates a table in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   tb = the name of the table to create
   *   iter = boolean indicating whether or not to use a versioning iterator
   *   tt = string corresponding to either LOGICAL or MILLIS for the time type
   *
   * Returns: R_NilValue
   */
  SEXP accum_create_table(SEXP l, SEXP r, SEXP tb, SEXP iter, SEXP tt)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    int verIter = (int)LOGICAL(iter)[0];
    const char *timetype = CHAR(STRING_ELT(tt,0));
    TimeType::type type;
    if (NULL != timetype && std::string(timetype).compare("LOGICAL") == 0)
    {
      type = TimeType::LOGICAL;
    }
    else
    {
      type = TimeType::MILLIS;
    }
    try
    {
      client->createTable(*login,tbn,(verIter!=0),type);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Clones a table in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to clone
   *   n = the name of the clone
   *   f = boolean indicating whether or not to flush the table firsr
   *   p = map of properties to set on the new table
   *   e = list of properties to exclude from the new table
   *
   * Returns: R_NilValue
   */
  SEXP accum_clone_table(SEXP l, SEXP r, SEXP t, SEXP n, SEXP f, SEXP p, SEXP e)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string newTable(CHAR(STRING_ELT(n,0)));
    bool flush = LOGICAL(f)[0];

    std::map<std::string, std::string> properties;
    if (p && LENGTH(p))
    {
      SEXP names = getAttrib(p, R_NamesSymbol);
      for (int i = 0; i < LENGTH(p); i++)
      {
        std::string name(CHAR(STRING_ELT(names,i)));
        std::string value(CHAR(STRING_ELT(p,i)));
        properties[name] = value; 
      }
    }
    std::set<std::string> exclude;
    if (e && LENGTH(e))
    {
      for (int i = 0; i < LENGTH(e); i++)
      {
        std::string value(CHAR(STRING_ELT(e,i)));
        exclude.insert(value); 
      }
    }

    try
    {
      client->cloneTable(*login,table,newTable,flush,properties,exclude);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Deletes a table in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   tb = the name of the table to delete
   *
   * Returns: R_NilValue
   */
  SEXP accum_delete_table(SEXP l, SEXP r, SEXP tb)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    try
    {
      client->deleteTable(*login, std::string(tbn));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Sets a table online in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   tb = the name of the table to online
   *
   * Returns: R_NilValue
   */
  SEXP accum_online_table(SEXP l, SEXP r, SEXP tb)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    try
    {
      client->onlineTable(*login, std::string(tbn));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Sets a table offline in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   tb = the name of the table to offline
   *
   * Returns: R_NilValue
   */
  SEXP accum_offline_table(SEXP l, SEXP r, SEXP tb)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    try
    {
      client->offlineTable(*login, std::string(tbn));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Renames a table in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   otb = the name of the table to rename
   *   ntb = the new name of the table
   *
   * Returns: R_NilValue
   */
  SEXP accum_rename_table(SEXP l, SEXP r, SEXP otb, SEXP ntb)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *oldt = CHAR(STRING_ELT(otb,0));
    const char *newt = CHAR(STRING_ELT(ntb,0));
    try
    {
      client->renameTable(*login, std::string(oldt),std::string(newt));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Checks the existence of the table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   tb = the name of the table to check
   *
   * Returns: A boolean indicating the existence of the table
   */
  SEXP accum_table_exists(SEXP l, SEXP r, SEXP tb)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *tbn = CHAR(STRING_ELT(tb,0));
    SEXP result = R_NilValue;
    try
    {
      if (client->tableExists(*login, std::string(tbn)))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }


  /**
   * Creates a user in the Accumulo instance.
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the name of the user to create
   *   p = the password to assign
   *
   * Returns: R_NilValue
   */
  SEXP accum_create_user(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *user = CHAR(STRING_ELT(u,0));
    const char *pass = CHAR(STRING_ELT(p,0));
    try
    {
      client->createLocalUser(*login,std::string(user),std::string(pass));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Deletes a user in the Accumulo instance.
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the name of the user to delete
   *
   * Returns: R_NilValue
   */
  SEXP accum_delete_user(SEXP l, SEXP r, SEXP u)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *user = CHAR(STRING_ELT(u,0));
    try
    {
      client->dropLocalUser(*login,std::string(user));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Changes a user's password in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the name of the user to modify
   *   p = the password to assign
   *
   * Returns: R_NilValue
   */
  SEXP accum_set_password(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *user = CHAR(STRING_ELT(u,0));
    const char *pass = CHAR(STRING_ELT(p,0));
    try
    {
      client->changeLocalUserPassword(*login,std::string(user),std::string(pass));
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Lists the valid users in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A vector of user names
   */
  SEXP accum_list_users(SEXP l, SEXP r)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::set<std::string> users;
    try
    {
      client->listLocalUsers(users,*login);
      if (users.size()>0)
      {
        PROTECT(result = Rf_allocVector(STRSXP,users.size()));
        UNPROTECT(1);
        int i = 0;
	for (std::set<std::string>::iterator it = users.begin(); it != users.end(); ++it)
        {
          SET_STRING_ELT(result, i++, Rf_mkChar(static_cast<const char*>((*it).c_str())));
        }
      }
    } 
    catch( TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Lists the authorizations assigned to a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to list authorizations for
   *
   * Returns: A vector of authorization strings
   */
  SEXP accum_get_auths(SEXP l, SEXP r, SEXP u)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *user = CHAR(STRING_ELT(u,0));
    std::vector<std::string> auths;
    try
    {
      client->getUserAuthorizations(auths,*login,std::string(user));
      if (auths.size()>0)
      {
        PROTECT(result = Rf_allocVector(STRSXP,auths.size()));
        UNPROTECT(1);
	for (int i = 0; i < (int)(auths.size()); i++)
        {
          SET_STRING_ELT(result, i, Rf_mkChar(static_cast<const char*>(auths[i].c_str())));
        }
      }
    } 
    catch( TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Sets the authorizations assigned to a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to set authorizations for
   *   a = a vector of authorization strings to set
   *
   * Returns: R_NilValue
   */
  SEXP accum_set_auths(SEXP l, SEXP r, SEXP u, SEXP a)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    const char *user = CHAR(STRING_ELT(u,0));

    std::set<std::string> auths;
    for (int i = 0; i < LENGTH(a); i++)
    {
      SEXP authname = VECTOR_ELT(a,i);
      auths.insert(std::string(CHAR(STRING_ELT(authname,0))));
    }
    try
    {
      client->changeUserAuthorizations(*login,std::string(user),auths);
    } 
    catch( TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Grants a system-level permission to a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to set grant permission to
   *   p = the permission to set
   *
   * Returns: R_NilValue
   */
  SEXP accum_grant_system_permission(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    SystemPermission::type type = getSysPerm(perm);
    try
    {
      client->grantSystemPermission(*login,user,type);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Grants a table-level permission to a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to set grant permission to
   *   p = the permission to set
   *
   * Returns: R_NilValue
   */
  SEXP accum_grant_table_permission(SEXP l, SEXP r, SEXP u, SEXP t, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    TablePermission::type type = getTablePerm(perm);
    try
    {
      client->grantTablePermission(*login,user,table,type);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Checks to see if a user has a system-level permission in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to check permission for
   *   p = the permission to check
   *
   * Returns: A boolean indicating whether or not the user has the specified permission
   */
  SEXP accum_has_system_permission(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    SystemPermission::type type = getSysPerm(perm);
    SEXP result = R_NilValue;
    try
    {
      if (client->hasSystemPermission(*login,user,type))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  /**
   * Checks to see if a user has a table-level permission in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to check permission for
   *   p = the permission to check
   *
   * Returns: A boolean indicating whether or not the user has the specified permission
   */
  SEXP accum_has_table_permission(SEXP l, SEXP r, SEXP u, SEXP t, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    TablePermission::type type = getTablePerm(perm);
    SEXP result = R_NilValue;
    try
    {
      if (client->hasTablePermission(*login,user,table,type))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  /**
   * Revokes a system-level permission from a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to revoke permission from
   *   p = the permission to revoke
   *
   * Returns: R_NilValue
   */
  SEXP accum_revoke_system_permission(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    SystemPermission::type type = getSysPerm(perm);
    try
    {
      client->revokeSystemPermission(*login,user,type);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Revokes a table-level permission from a user in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to revoke permission from
   *   p = the permission to revoke
   *
   * Returns: R_NilValue
   */
  SEXP accum_revoke_table_permission(SEXP l, SEXP r, SEXP u, SEXP t, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string perm(CHAR(STRING_ELT(p,0)));
    TablePermission::type type = getTablePerm(perm);
    try
    {
      client->revokeTablePermission(*login,user,table,type);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Checks the validity of a login in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   u = the user to authenticate
   *   p = the user's password
   *
   * Returns: A boolean indicating the validity of the login
   */
  SEXP accum_authenticate_user(SEXP l, SEXP r, SEXP u, SEXP p)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string user(CHAR(STRING_ELT(u,0)));
    std::string password(CHAR(STRING_ELT(p,0)));

    map<std::string, std::string> props;
    props["password"] = password;
    
    try{
      if (client->authenticateUser(*login, user, props))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Sets a system property on the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   n = the property name
   *   v = the property value
   *
   * Returns: R_NilValue
   */
  SEXP accum_set_property(SEXP l, SEXP r, SEXP n, SEXP v)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string name(CHAR(STRING_ELT(n,0)));
    std::string value(CHAR(STRING_ELT(v,0)));

    try
    {
      client->setProperty(*login,name,value);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Removes a system property on the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   n = the property name
   *   v = the property value
   *
   * Returns: R_NilValue
   */
  SEXP accum_remove_property(SEXP l, SEXP r, SEXP n)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string name(CHAR(STRING_ELT(n,0)));

    try
    {
      client->removeProperty(*login,name);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Sets a table property on the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   n = the property name
   *   v = the property value
   *
   * Returns: R_NilValue
   */
  SEXP accum_set_table_property(SEXP l, SEXP r, SEXP t, SEXP n, SEXP v)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string name(CHAR(STRING_ELT(n,0)));
    std::string value(CHAR(STRING_ELT(v,0)));

    try
    {
      client->setTableProperty(*login,table,name,value);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Removes a table property on the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   n = the property name
   *   v = the property value
   *
   * Returns: R_NilValue
   */
  SEXP accum_remove_table_property(SEXP l, SEXP r, SEXP t, SEXP n)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string name(CHAR(STRING_ELT(n,0)));

    try
    {
      client->removeTableProperty(*login,table,name);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Gets a list of the tablet servers in the Accumulo instance
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A vector of tablet server addresses
   */
  SEXP accum_get_tablet_servers(SEXP l, SEXP r)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::vector<std::string> tablets;
    try
    {
      client->getTabletServers(tablets,*login);
      if (tablets.size()>0)
      {
        PROTECT(result = Rf_allocVector(STRSXP,tablets.size()));
        UNPROTECT(1);
	for (int i = 0; i < (int)tablets.size(); i++)
        {
          SET_STRING_ELT(result, i, Rf_mkChar(static_cast<const char*>((tablets[i]).c_str())));
        }
      }
    } 
    catch( TException &tx) 
    {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Pings a tablet server in the Accumulo instance.  Throws exception if server cannot be reached.
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the address of the tablet server to ping
   *
   * Returns: R_NilValue
   */
  SEXP accum_ping_tablet_server(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string tablet(CHAR(STRING_ELT(t,0)));

    try
    {
      client->pingTabletServer(*login,tablet);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /** 
   * Merges tablets for a given table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to merge tablets for
   *   s = the row key to start the merge from
   *   e = the row key to end the merge at
   *
   * Returns: R_NilValue
  */
  SEXP accum_merge_tablets(SEXP l, SEXP r, SEXP t, SEXP s, SEXP e)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string start(CHAR(STRING_ELT(s,0)));
    std::string end(CHAR(STRING_ELT(e,0)));

    try
    {
      client->mergeTablets(*login,table,start,end);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /** 
   * Adds a constraint to a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to add the constraint to
   *   c = the class name of the constraint to add
   *
   * Returns: An integer token representing the added constraint
  */
  SEXP accum_add_contraint(SEXP l, SEXP r, SEXP t, SEXP c)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string constraint(CHAR(STRING_ELT(c,0)));

    SEXP result = R_NilValue;
    try
    {
      int32_t retval = client->addConstraint(*login,table,constraint);
      result = Rf_ScalarInteger(retval);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Removes a constraint from a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to remove the constraint from
   *   c = The integer token representing the constraint (returned
   *       from accum_add_constraint
   *
   * Returns: R_NilValue
  */
  SEXP accum_remove_contraint(SEXP l, SEXP r, SEXP t, SEXP c)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    int32_t constraint = INTEGER(c)[0];

    try
    {
      client->removeConstraint(*login,table,constraint);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Clears the locator cache for a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to clear the cache for
   *
   * Returns: R_NilValue
  */
  SEXP accum_clear_locator_cache(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    try
    {
      client->clearLocatorCache(*login,table);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Imports a table from an export
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to import
   *   i = the directory in HDFS that contains the exported table
   *
   * Returns: R_NilValue
  */
  SEXP accum_import_table(SEXP l, SEXP r, SEXP t, SEXP i)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string importDir(CHAR(STRING_ELT(i,0)));

    try
    {
      client->importTable(*login,table,importDir);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Imports a table from an export directory
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to import
   *   i = the directory in HDFS that contains the exported table
   *   f = the directory in HDFS to store failed files in
   *   s = boolean indicating whether or not to set the timestamp 
   *
   * Returns: R_NilValue
  */
  SEXP accum_import_directory(SEXP l, SEXP r, SEXP t, SEXP i, SEXP f, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string importDir(CHAR(STRING_ELT(i,0)));
    std::string failureDir(CHAR(STRING_ELT(f,0)));
    bool setTime = LOGICAL(s)[0];

    try
    {
      client->importDirectory(*login,table,importDir,failureDir,setTime);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Exports a table to a directory
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the name of the table to export
   *   e = the directory in HDFS to export the table to
   *
   * Returns: R_NilValue
  */
  SEXP accum_export_table(SEXP l, SEXP r, SEXP t, SEXP e)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string exportDir(CHAR(STRING_ELT(e,0)));

    try
    {
      client->exportTable(*login,table,exportDir);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Gets a list of site properties
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A list of names and values representing the site properties
  */
  SEXP accum_get_site_configuration(SEXP l, SEXP r)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::map<std::string,std::string> conf;
    try
    {
      client->getSiteConfiguration(conf,*login);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, conf.size()));
      PROTECT(values = Rf_allocVector(STRSXP, conf.size()));
      int i = 0;
      for (std::map<std::string,std::string>::iterator iter = conf.begin();
           iter != conf.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        SET_STRING_ELT(values, i++, Rf_mkChar(static_cast<const char*>((iter->second).c_str())));  
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Gets a list of system properties
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A list of names and values representing the system properties
  */
  SEXP accum_get_system_configuration(SEXP l, SEXP r)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::map<std::string,std::string> conf;
    try
    {
      client->getSystemConfiguration(conf,*login);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, conf.size()));
      PROTECT(values = Rf_allocVector(STRSXP, conf.size()));
      int i = 0;
      for (std::map<std::string,std::string>::iterator iter = conf.begin();
           iter != conf.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        SET_STRING_ELT(values, i++, Rf_mkChar(static_cast<const char*>((iter->second).c_str())));  
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Gets a map of table IDs
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *
   * Returns: A list of names and values representing the table IDs
  */
  SEXP accum_table_id_map(SEXP l, SEXP r)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::map<std::string,std::string> tableIdMap;
    try
    {
      client->tableIdMap(tableIdMap,*login);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, tableIdMap.size()));
      PROTECT(values = Rf_allocVector(STRSXP, tableIdMap.size()));
      int i = 0;
      for (std::map<std::string,std::string>::iterator iter = tableIdMap.begin();
           iter != tableIdMap.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        SET_STRING_ELT(values, i++, Rf_mkChar(static_cast<const char*>((iter->second).c_str())));  
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Gets the disk usage for a given table or tables
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table(s) to get disk usage for
   *
   * Returns: A list of names and values representing the disk usage per table
  */
  SEXP accum_disk_usage(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::set<std::string> tables;
    for (int i = 0; i < LENGTH(t); i++)
    {
      std::string table(CHAR(STRING_ELT(t,i)));
      tables.insert(table);
    }
    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::vector<DiskUsage> usage;
    try
    {
      client->getDiskUsage(usage,*login,tables);
      PROTECT(result = Rf_allocVector(VECSXP, 2));
      PROTECT(names = Rf_allocVector(STRSXP, usage.size()));
      PROTECT(values = Rf_allocVector(VECSXP, usage.size()));
      for (unsigned int i = 0; i < usage.size(); i++)
      {
        DiskUsage du = usage[i];
        SET_STRING_ELT(names,i, Rf_mkChar(du.tables[0].c_str()));
        SET_VECTOR_ELT(values,i, Rf_ScalarInteger(du.usage));
      }
      SET_VECTOR_ELT(result,0, names);
      SET_VECTOR_ELT(result,1, values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Adds splits to a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table(s) to add splits to
   *   s = the list of split points to add
   *
   * Returns: R_NilValue
  */
  SEXP accum_add_splits(SEXP l, SEXP r, SEXP t, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::set<std::string> splits;
    for (int i = 0; i < LENGTH(s); i++)
    {
      std::string split(CHAR(STRING_ELT(s,i)));
      splits.insert(split);
    }
    try
    {
      client->addSplits(*login,table,splits);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  /** 
   * Lists the splits for a given table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table(s) to list the splits for
   *   m = the maximum number of splits to return
   *
   * Returns: A list of split points for a given table
  */
  SEXP accum_list_splits(SEXP l, SEXP r, SEXP t, SEXP m)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    int32_t max = INTEGER(m)[0];    

    SEXP result = R_NilValue;
    std::vector<std::string> splits;
    try
    {
      client->listSplits(splits,*login,table,max);
      PROTECT(result = Rf_allocVector(STRSXP, splits.size()));
      for (unsigned int i = 0; i < splits.size(); i++)
      {
        SET_STRING_ELT(result,i, Rf_mkChar(splits[i].c_str()));
      }
      UNPROTECT(1);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /** 
   * Gets a list of table properties
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to list properties for
   *
   * Returns: A list of names and values representing the table properties
  */
  SEXP accum_get_table_properties(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::map<std::string,std::string> properties;
    try
    {
      client->getTableProperties(properties,*login,table);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, properties.size()));
      PROTECT(values = Rf_allocVector(STRSXP, properties.size()));
      int i = 0;
      for (std::map<std::string,std::string>::iterator iter = properties.begin();
           iter != properties.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        SET_STRING_ELT(values, i++, Rf_mkChar(static_cast<const char*>((iter->second).c_str())));  
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Gets a list of table constraints
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to list constraints for
   *
   * Returns: A list of names and values representing the constraints
  */
  SEXP accum_list_constraints(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    std::map<std::string,int32_t> constraints;
    try
    {
      client->listConstraints(constraints,*login,table);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, constraints.size()));
      PROTECT(values = Rf_allocVector(VECSXP, constraints.size()));
      int i = 0;
      for (std::map<std::string,int32_t>::iterator iter = constraints.begin();
           iter != constraints.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        SET_VECTOR_ELT(values, i++, Rf_ScalarInteger(iter->second));  
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  /** 
   * Tests the loading of a Java class
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   c = the Java class name to load
   *   a = the type name to assign to the loaded class
   *
   * Returns: True if the class can be loaded, false otherwise
  */
  SEXP accum_test_class_load(SEXP l, SEXP r, SEXP c, SEXP a)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string classname(CHAR(STRING_ELT(c,0)));
    std::string asType(CHAR(STRING_ELT(a,0)));

    try{
      if (client->testClassLoad(*login, classname, asType))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /** 
   * Tests the loading of a Java class in a table context
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table name to load the class on
   *   c = the Java class name to load
   *   a = the type name to assign to the loaded class
   *
   * Returns: True if the class can be loaded, false otherwise
  */
  SEXP accum_test_table_class_load(SEXP l, SEXP r, SEXP t, SEXP c, SEXP a)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string classname(CHAR(STRING_ELT(c,0)));
    std::string asType(CHAR(STRING_ELT(t,0)));

    try{
      if (client->testTableClassLoad(*login, table, classname, asType))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  SEXP accum_attach_iterator(SEXP l, SEXP r, SEXP t, SEXP n, SEXP c, SEXP p, SEXP q, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string name(CHAR(STRING_ELT(n,0)));
    std::string iterClass(CHAR(STRING_ELT(c,0)));

    IteratorSetting setting;
    setting.name = name;
    setting.iteratorClass = iterClass;
    setting.priority = INTEGER(p)[0];    

    if (NULL != q)
    {
      SEXP names = getAttrib(q, R_NamesSymbol);
      for (int i = 0; i < LENGTH(q); i++)
      {
        std::string name(CHAR(STRING_ELT(names,i)));
        std::string value(CHAR(STRING_ELT(q,i)));
        setting.properties[name] = value; 
      }
    }

    std::set<IteratorScope::type> scopes;
    if (NULL != s)
    {
      for (int i = 0; i < LENGTH(s); i++)
      {
        std::string scope(CHAR(STRING_ELT(s,i)));
        scopes.insert(getIteratorScope(scope));
      }
    }
    try
    {
      client->attachIterator(*login,table,setting,scopes);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  SEXP accum_check_iterator(SEXP l, SEXP r, SEXP t, SEXP n, SEXP c, SEXP p, SEXP q, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string name(CHAR(STRING_ELT(n,0)));
    std::string iterClass(CHAR(STRING_ELT(c,0)));

    IteratorSetting setting;
    setting.name = name;
    setting.iteratorClass = iterClass;
    setting.priority = INTEGER(p)[0];    

    if (NULL != q)
    {
      SEXP names = getAttrib(q, R_NamesSymbol);
      for (int i = 0; i < LENGTH(q); i++)
      {
        std::string name(CHAR(STRING_ELT(names,i)));
        std::string value(CHAR(STRING_ELT(q,i)));
        setting.properties[name] = value; 
      }
    }

    std::set<IteratorScope::type> scopes;
    if (NULL != s)
    {
      for (int i = 0; i < LENGTH(s); i++)
      {
        std::string scope(CHAR(STRING_ELT(s,i)));
        scopes.insert(getIteratorScope(scope));
      }
    }
    try
    {
      client->checkIteratorConflicts(*login,table,setting,scopes);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  SEXP accum_remove_iterator(SEXP l, SEXP r, SEXP t, SEXP n, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string name(CHAR(STRING_ELT(n,0)));

    std::set<IteratorScope::type> scopes;
    if (NULL != s)
    {
      for (int i = 0; i < LENGTH(s); i++)
      {
        std::string scope(CHAR(STRING_ELT(s,i)));
        scopes.insert(getIteratorScope(scope));
      }
    }
    try
    {
      client->removeIterator(*login,table,name,scopes);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  SEXP accum_list_iterators(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    SEXP scopes = R_NilValue;
    std::map<std::string,std::set<IteratorScope::type> > iterators;
    try
    {
      client->listIterators(iterators,*login,table);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, iterators.size()));
      PROTECT(values = Rf_allocVector(VECSXP, iterators.size()));
      int i = 0;
      for (std::map<std::string,std::set<IteratorScope::type> >::iterator iter = iterators.begin();
           iter != iterators.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        PROTECT(scopes = Rf_allocVector(STRSXP, iter->second.size()));
        int j = 0;
        for (std::set<IteratorScope::type>::iterator inner = iter->second.begin();
             inner != iter->second.end(); ++inner)
        {
          SET_STRING_ELT(scopes, j++, Rf_mkChar(_IteratorScope_VALUES_TO_NAMES[(int)(*inner)]));
        }
        SET_VECTOR_ELT(values, i++, scopes);  
        UNPROTECT(1);
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  SEXP accum_get_iterator_setting(SEXP l, SEXP r, SEXP t, SEXP n, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string iterName(CHAR(STRING_ELT(n,0)));
    std::string sc(CHAR(STRING_ELT(s,0)));
    IteratorScope::type scope = getIteratorScope(sc);

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    SEXP propnames = R_NilValue;
    SEXP propvalues = R_NilValue;
    IteratorSetting setting;
    try
    {
      client->getIteratorSetting(setting,*login,table,iterName,scope);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, 4));
      PROTECT(values = Rf_allocVector(VECSXP, 4));
      SET_STRING_ELT(names, 0, Rf_mkChar("name"));  
      SET_VECTOR_ELT(values, 0, Rf_ScalarString(Rf_mkChar(setting.name.c_str())));  
      
      SET_STRING_ELT(names, 1, Rf_mkChar("iteratorClass"));  
      SET_VECTOR_ELT(values, 1, Rf_ScalarString(Rf_mkChar(setting.iteratorClass.c_str())));  

      SET_STRING_ELT(names, 2, Rf_mkChar("priority"));  
      SET_VECTOR_ELT(values, 2, Rf_ScalarInteger(setting.priority));  

      SET_STRING_ELT(names, 3, Rf_mkChar("properties"));  

      PROTECT(propnames = Rf_allocVector(STRSXP,setting.properties.size()));
      PROTECT(propvalues = Rf_allocVector(STRSXP,setting.properties.size()));
      int i = 0;
      for (std::map<std::string,std::string>::iterator iter = setting.properties.begin();
           iter != setting.properties.end(); ++iter)
      {
        SET_STRING_ELT(propnames, i, Rf_mkChar(static_cast<const char *>(iter->first.c_str())));   
        SET_STRING_ELT(propvalues, i, Rf_mkChar(static_cast<const char *>(iter->second.c_str())));   
      }
      setAttrib(propvalues, R_NamesSymbol, propnames);
      SET_VECTOR_ELT(values, 3, propvalues);

      SET_VECTOR_ELT(result, 0, names);
      SET_VECTOR_ELT(result, 1, values);
      UNPROTECT(5);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  SEXP accum_get_locality_groups(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    SEXP result = R_NilValue;
    SEXP names = R_NilValue;
    SEXP values = R_NilValue;
    SEXP group = R_NilValue;
    std::map<std::string,std::set<std::string> > localityGroups;
    try
    {
      client->getLocalityGroups(localityGroups,*login,table);
      PROTECT(result = Rf_allocVector(VECSXP, 2));  
      PROTECT(names = Rf_allocVector(STRSXP, localityGroups.size()));
      PROTECT(values = Rf_allocVector(VECSXP, localityGroups.size()));
      int i = 0;
      for (std::map<std::string,std::set<std::string> >::iterator iter = localityGroups.begin();
           iter != localityGroups.end(); ++iter)
      {
        SET_STRING_ELT(names, i, Rf_mkChar(static_cast<const char*>((iter->first).c_str())));  
        PROTECT(group = Rf_allocVector(STRSXP, iter->second.size()));
        int j = 0;
        for (std::set<std::string>::iterator inner = iter->second.begin();
             inner != iter->second.end(); ++inner)
        {
          SET_STRING_ELT(group, j++, Rf_mkChar(static_cast<const char *>((*inner).c_str())));
        }
        SET_VECTOR_ELT(values, i++, group);  
        UNPROTECT(1);
      }
      SET_VECTOR_ELT(result,0,names);
      SET_VECTOR_ELT(result,1,values);
      UNPROTECT(3);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(result);
  }

  SEXP accum_set_locality_groups(SEXP l, SEXP r, SEXP t, SEXP g)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    std::map<std::string, std::set<std::string> > localities;
    if (NULL != g)
    {
      SEXP names = getAttrib(g, R_NamesSymbol);
      for (int i = 0; i < LENGTH(g); i++)
      {
        std::string name(CHAR(STRING_ELT(names,i)));
        std::set<std::string> groups;
        SEXP gr = VECTOR_ELT(g,i);
        for (int j = 0; j < LENGTH(gr); j++)
        {
          std::string value(CHAR(STRING_ELT(gr,j)));
          groups.insert(value);
        }
        localities[name] = groups;
      }
    }

    try
    {
      client->setLocalityGroups(*login,table,localities);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }

    return(R_NilValue);
  }

  SEXP accum_get_active_scans(SEXP l, SEXP r, SEXP t)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string tablet(CHAR(STRING_ELT(t,0)));

    std::vector<ActiveScan> scans;

    try{
      client->getActiveScans(scans, *login, tablet);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  SEXP accum_get_active_compactions(SEXP l, SEXP r, SEXP t)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string tablet(CHAR(STRING_ELT(t,0)));

    std::vector<ActiveCompaction> compactions;

    try{
      client->getActiveCompactions(compactions, *login, tablet);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  SEXP accum_flush_table(SEXP l, SEXP r, SEXP t, SEXP s, SEXP e, SEXP w)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string start(CHAR(STRING_ELT(s,0)));
    std::string end(CHAR(STRING_ELT(e,0)));
    bool wait = LOGICAL(w)[0];

    try
    {
      client->flushTable(*login,table,start,end,wait);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  SEXP accum_create_scanner(SEXP l, SEXP r, SEXP t, SEXP o)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    ScanOptions scanopts;
    if (o && LENGTH(o))
    {
      getScanOptions(o,scanopts);
    }

    std::string *scanner = new std::string();

    try{
      client->createScanner(*scanner, *login, table, scanopts);
      PROTECT(result = R_MakeExternalPtr((void*)scanner, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  SEXP accum_create_batch_scanner(SEXP l, SEXP r, SEXP t, SEXP o)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    BatchScanOptions scanopts;
    if (o && LENGTH(o))
    {
      getBatchScanOptions(o,scanopts);
    }

    std::string *scanner = new std::string();

    try{
      client->createBatchScanner(*scanner, *login, table, scanopts);
      PROTECT(result = R_MakeExternalPtr((void*)scanner, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Checks to see if a scanner has more results
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   s = the pointer to the scanner token
   *
   * Returns: A boolean indicating the existence of more results
   */
  SEXP accum_has_next(SEXP r, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *scanner = static_cast<std::string*>(R_ExternalPtrAddr(s));

    SEXP result = R_NilValue;
    try
    {
      if (client->hasNext(*scanner))
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 1;
        UNPROTECT(1);
      }
      else
      {
        PROTECT(result = Rf_allocVector(LGLSXP,1));
        LOGICAL(result)[0] = 0;
        UNPROTECT(1);
      }   
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  
  /**
   * Gets the next entry from a scanner
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   s = the pointer to the scanner token
   *
   * Returns: A list of representing the next entry
   */
  SEXP accum_next_entry(SEXP r, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *scanner = static_cast<std::string*>(R_ExternalPtrAddr(s));

    SEXP names = R_NilValue;
    SEXP result = R_NilValue;
    try
    {
      KeyValueAndPeek nextEntry;
      client->nextEntry(nextEntry,*scanner);
      PROTECT(names = Rf_allocVector(STRSXP, 6));
      PROTECT(result = Rf_allocVector(VECSXP, 6));
      SET_STRING_ELT(names, 0, Rf_mkChar("row"));
      SET_VECTOR_ELT(result,0,Rf_ScalarString(Rf_mkChar(nextEntry.keyValue.key.row.c_str())));

      SET_STRING_ELT(names, 1, Rf_mkChar("colFamily"));
      SET_VECTOR_ELT(result,1,Rf_ScalarString(Rf_mkChar(nextEntry.keyValue.key.colFamily.c_str())));

      SET_STRING_ELT(names, 2, Rf_mkChar("colQualifier"));
      SET_VECTOR_ELT(result,2,Rf_ScalarString(Rf_mkChar(nextEntry.keyValue.key.colQualifier.c_str())));

      SET_STRING_ELT(names, 3, Rf_mkChar("colVisibility"));
      SET_VECTOR_ELT(result,3,Rf_ScalarString(Rf_mkChar(nextEntry.keyValue.key.colVisibility.c_str())));

      SET_STRING_ELT(names, 4, Rf_mkChar("timestamp"));
      SET_VECTOR_ELT(result,4,Rf_ScalarInteger(nextEntry.keyValue.key.timestamp));

      SET_STRING_ELT(names, 5, Rf_mkChar("value"));
      SET_VECTOR_ELT(result,5,Rf_ScalarString(Rf_mkChar(nextEntry.keyValue.value.c_str())));

      setAttrib(result, R_NamesSymbol, names);
      UNPROTECT(2);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  /**
   * Gets the next k entries from a scanner
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   s = the pointer to the scanner token
   *   k = the pointer to the number of entries to retrieve
   *
   * Returns: A list of key-value pairs that form the next entry
   */
  SEXP accum_next_k(SEXP r, SEXP s, SEXP k)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *scanner = static_cast<std::string*>(R_ExternalPtrAddr(s));
    int32_t numResults = INTEGER(k)[0];

    SEXP retval = R_NilValue;
    SEXP results = R_NilValue;
    SEXP innerNames = R_NilValue;
    SEXP outerNames = R_NilValue;
    try
    {
      ScanResult res;
      client->nextK(res, *scanner, numResults);
      PROTECT(retval = Rf_allocVector(VECSXP,2));
      PROTECT(outerNames = Rf_allocVector(STRSXP,2));

      SET_STRING_ELT(outerNames, 0, Rf_mkChar("results"));
      SET_STRING_ELT(outerNames, 1, Rf_mkChar("more"));

      if (res.results.size())
      {
        PROTECT(results = Rf_allocVector(VECSXP,res.results.size()));
        PROTECT(innerNames = Rf_allocVector(STRSXP,6)); 
        SET_STRING_ELT(innerNames, 0, Rf_mkChar("row"));
        SET_STRING_ELT(innerNames, 1, Rf_mkChar("colFamily"));
        SET_STRING_ELT(innerNames, 2, Rf_mkChar("colQualifier"));
        SET_STRING_ELT(innerNames, 3, Rf_mkChar("colVisibility"));
        SET_STRING_ELT(innerNames, 4, Rf_mkChar("timestamp"));
        SET_STRING_ELT(innerNames, 5, Rf_mkChar("value"));
        for (unsigned int i = 0; i < res.results.size(); i++)
        {
          SEXP kv;
          PROTECT(kv = Rf_allocVector(VECSXP,6));
        
          SET_VECTOR_ELT(kv,0,Rf_ScalarString(Rf_mkChar(res.results[i].key.row.c_str())));
          SET_VECTOR_ELT(kv,1,Rf_ScalarString(Rf_mkChar(res.results[i].key.colFamily.c_str())));
          SET_VECTOR_ELT(kv,2,Rf_ScalarString(Rf_mkChar(res.results[i].key.colQualifier.c_str())));
          SET_VECTOR_ELT(kv,3,Rf_ScalarString(Rf_mkChar(res.results[i].key.colVisibility.c_str())));
          SET_VECTOR_ELT(kv,4,Rf_ScalarInteger(res.results[i].key.timestamp));
          SET_VECTOR_ELT(kv,5,Rf_ScalarString(Rf_mkChar(res.results[i].value.c_str())));
          
          setAttrib(kv, R_NamesSymbol, innerNames);
          SET_VECTOR_ELT(results,i,kv);
          UNPROTECT(1);
        }
        SET_VECTOR_ELT(retval,0,results);
        UNPROTECT(2);
      } 
      SET_VECTOR_ELT(retval,1,Rf_ScalarLogical(res.more));
      setAttrib(retval, R_NamesSymbol, outerNames);
      UNPROTECT(2);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(retval);
  }

  /**
   * Closes a scanner and deallocates resources
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   s = the pointer to the scanner token
   *
   * Returns: R_NilValue
   */
  SEXP accum_close_scanner(SEXP r, SEXP s)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *scanner = static_cast<std::string*>(R_ExternalPtrAddr(s));

    try
    {
      client->closeScanner(*scanner);
      delete scanner;
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   *  Deletes rows from a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to delete rows from
   *   s = the row key to begin the delete at
   *   e = the row key to end the delete at
   *
   * Returns: R_NilValue
   */
  SEXP accum_delete_rows(SEXP l, SEXP r, SEXP t, SEXP s, SEXP e)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string start(CHAR(STRING_ELT(s,0)));
    std::string end(CHAR(STRING_ELT(e,0)));

    try
    {
      client->deleteRows(*login,table,start,end);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }



  /**
   *  Creates a writer for the specified table
   *
   *  Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to create the writer for
   *   o = the options to set on the writer
   *
   *  Returns: A pointer to a writer token
   */
  SEXP accum_create_writer(SEXP l, SEXP r, SEXP t, SEXP o)
  {
    SEXP result = R_NilValue;
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    WriterOptions wropts; 
    if (o && o != R_NilValue)
    {
      int64_t intval = INTEGER(getListElement(o,"maxMemory"))[0];
      if (intval) wropts.maxMemory = intval;
      intval = INTEGER(getListElement(o,"latencyMs"))[0];
      if (intval) wropts.latencyMs = intval;
      intval = INTEGER(getListElement(o,"timeoutMs"))[0];
      if (intval) wropts.timeoutMs = intval;
      intval = INTEGER(getListElement(o,"threads"))[0];
      if (intval) wropts.threads = intval;
    }

    std::string *writer = new std::string();

    try{
      client->createWriter(*writer, *login, table, wropts);
      PROTECT(result = R_MakeExternalPtr((void*)writer, R_NilValue,R_NilValue));
      UNPROTECT(1);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(result);
  }

  /**
   * Flushes a writer
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   w = the pointer to the writer token
   *
   * Returns: R_NilValue
   */
  SEXP accum_flush_writer(SEXP r, SEXP w)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *writer = static_cast<std::string*>(R_ExternalPtrAddr(w));

    try
    {
      client->flush(*writer);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   * Closes a writer and deallocates resources
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   w = the pointer to the writer token
   *
   * Returns: R_NilValue
   */
  SEXP accum_close_writer(SEXP r, SEXP w)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *writer = static_cast<std::string*>(R_ExternalPtrAddr(w));

    try
    {
      client->closeWriter(*writer);
      delete writer;
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  
  /**
   *  Sends an update to the Accumulo instance
   *
   *  Params:
   *   r = the pointer to the AccumuloProxyClient
   *   w = the pointer to the writer token
   *   c = the cell values to update
   *
   *  Returns: R_NilValue
   */
  SEXP accum_update(SEXP r, SEXP w, SEXP c)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *writer = static_cast<std::string*>(R_ExternalPtrAddr(w));

    std::map<std::string, std::vector<ColumnUpdate> > updates; 

    int totalUpdates = LENGTH(c);
    for (int i = 0; i < totalUpdates; i++)
    {
      SEXP update = VECTOR_ELT(c,i);
      std::string key(CHAR(STRING_ELT(getListElement(update, "row"),0)));
      ColumnUpdate cell;
      cell.__set_colFamily(CHAR(STRING_ELT(getListElement(update, "colFamily"),0)));
      cell.__set_colQualifier(CHAR(STRING_ELT(getListElement(update, "colQualifier"),0)));
      std::string vis(CHAR(STRING_ELT(getListElement(update, "colVisibility"),0)));
      if (!vis.empty()) cell.__set_colVisibility(vis);
      int64_t tstamp = INTEGER(getListElement(update,"timestamp"))[0];
      if (tstamp) cell.__set_timestamp(tstamp);
      bool del = LOGICAL(getListElement(update,"delete"))[0];
      if (del) cell.__set_deleteCell(del);
      cell.__set_value(CHAR(STRING_ELT(getListElement(update, "value"),0)));
      updates[key].push_back(cell);
    }

    try{
      client->update(*writer, updates);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   *  Sends an update to the Accumulo instance and flushes
   *
   *  Params:
   *   r = the pointer to the AccumuloProxyClient
   *   l = the pointer to the login token
   *   t = the table to update
   *   c = the cell values to update
   *
   *  Returns: R_NilValue
   */
  SEXP accum_update_and_flush(SEXP l, SEXP r, SEXP t, SEXP c)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));

    std::map<std::string, std::vector<ColumnUpdate> > updates; 
    int totalUpdates = LENGTH(c);
    for (int i = 0; i < totalUpdates; i++)
    {
      SEXP update = VECTOR_ELT(c,i);
      std::string key(CHAR(STRING_ELT(getListElement(update, "row"),0)));
      ColumnUpdate cell;
      cell.__set_colFamily(CHAR(STRING_ELT(getListElement(update, "colFamily"),0)));
      cell.__set_colQualifier(CHAR(STRING_ELT(getListElement(update, "colQualifier"),0)));
      std::string vis(CHAR(STRING_ELT(getListElement(update, "colVisibility"),0)));
      if (!vis.empty()) cell.__set_colVisibility(vis);
      int64_t tstamp = INTEGER(getListElement(update,"timestamp"))[0];
      if (tstamp) cell.__set_timestamp(tstamp);
      bool del = LOGICAL(getListElement(update,"delete"))[0];
      if (del) cell.__set_deleteCell(del);
      cell.__set_value(CHAR(STRING_ELT(getListElement(update, "value"),0)));
      updates[key].push_back(cell);
    }

    try{
      client->updateAndFlush(*login, table, updates);
    } catch(TException &tx) {
      Rf_error("raccumulo:: %s",tx.what());
    }
    return(R_NilValue);
  }

  /**
   *  Compacts a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to compact
   *   s = the row key to begin the compaction at
   *   e = the row key to end the compaction at
   *   i = a list of iterators to use
   *   f = whether or not to flush the table
   *   w = whether or not wait for the compaction
   *
   * Returns: R_NilValue
   */
  SEXP accum_compact_table(SEXP l, SEXP r, SEXP t, SEXP s, SEXP e, SEXP i, SEXP f, SEXP w)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string start(CHAR(STRING_ELT(s,0)));
    std::string end(CHAR(STRING_ELT(e,0)));

    bool flush = LOGICAL(f)[0];
    bool wait = LOGICAL(w)[0];

    std::vector<IteratorSetting> iteratorSettings;
    if (i && LENGTH(i))
    {
      for (int j = 0; j < LENGTH(i); j++)
      {
        SEXP it = VECTOR_ELT(i, j);
        boost::shared_ptr<IteratorSetting> is = getIteratorSetting(it);
        iteratorSettings.push_back(*is);
      }
    }

    try
    {
      client->compactTable(*login,table,start,end,iteratorSettings,flush,wait);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   *  Cancels a compaction on a table
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to cancel the compaction for
   *
   * Returns: R_NilValue
   */
  SEXP accum_cancel_compaction(SEXP l, SEXP r, SEXP t)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    try
    {
      client->cancelCompaction(*login,table);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(R_NilValue);
  }

  /**
   *  Gets a Range structure based on a row key
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   k = the row key to get the range for
   *
   * Returns: R_NilValue
   */
  SEXP accum_get_row_range(SEXP r,SEXP k)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string key(CHAR(STRING_ELT(k,0)));
    Range range;
    SEXP result = R_NilValue;
    try
    {
      client->getRowRange(range,key);
      result = getRangeAsList(range);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  /**
   *  Gets a list of ranges based on the given range, split
   *  by tablets
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to get ranges for
   *   s = the range to split
   *   m = the maximum number of splits
   *
   * Returns: A list of named lists representing ranges
   */
  SEXP accum_split_range_by_tablets(SEXP l, SEXP r, SEXP t, SEXP s, SEXP m)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    std::string table(CHAR(STRING_ELT(t,0)));
    int32_t max = INTEGER(m)[0];
    Range range;
    getRange(s,range);
    std::set<Range> ranges;
    SEXP result = R_NilValue;
    try
    {
      client->splitRangeByTablets(ranges,*login,table,range,max);
      if (ranges.size())
      {
        PROTECT(result = Rf_allocVector(VECSXP,ranges.size()));
        int i = 0;
        for (std::set<Range>::iterator iter = ranges.begin();
             iter != ranges.end(); ++iter)
        {
          SET_VECTOR_ELT(result, i++, getRangeAsList(*iter));
        }
        UNPROTECT(1);
      }
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  }

  /**
   *  Gets the key that follows the specified key
   * 
   * Params:
   *   r = the pointer to the AccumuloProxyClient
   *   k = the key to find the following key for
   *   p = the PartialKey value that describes what parts of the key to use
   *
   * Returns: A named list representing the following key
   */
  SEXP accum_get_following(SEXP r, SEXP k, SEXP p)
  {
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));

    std::string partial(CHAR(STRING_ELT(p,0)));
    PartialKey::type part = getPartialKey(partial);

    Key key;
    getKey(k, key);
    Key following;
    SEXP result = R_NilValue;
    try
    {
      client->getFollowing(following,key,part);
      result = getKeyAsList(following);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  } 

  /**
   *  Gets the maximum row for the table given the specified params
   * 
   * Params:
   *   l = the pointer to the login token
   *   r = the pointer to the AccumuloProxyClient
   *   t = the table to search for the max row in
   *   a = the authorizations to use for the search
   *   s = the row to start searching at
   *   i = indicates whether or not to include the start row
   *   e = the row to stop searching at
   *   j = indicates whether or not to include the stop row
   *
   * Returns: A named list representing the following key
   */
  SEXP accum_get_max_row(SEXP l, SEXP r, SEXP t, SEXP a, SEXP s, SEXP i, SEXP e, SEXP j)
  {
    std::string *login = static_cast<std::string*>(R_ExternalPtrAddr(l));
    AccumuloProxyClient *client  = static_cast<AccumuloProxyClient*>(R_ExternalPtrAddr(r));
    std::string table(CHAR(STRING_ELT(t,0)));
    std::string start(CHAR(STRING_ELT(s,0)));
    std::string stop(CHAR(STRING_ELT(e,0)));

    bool startInclusive = LOGICAL(i)[0];
    bool stopInclusive = LOGICAL(j)[0];

    std::set<std::string> auths;
    if (a && LENGTH(a))
    {
      for (int k = 0; k < LENGTH(a); k++)
      {
        SEXP authname = VECTOR_ELT(a,k);
        auths.insert(std::string(CHAR(STRING_ELT(authname,0))));
      }
    }
    std::string retval;
    SEXP result = R_NilValue;
    try
    {
      client->getMaxRow(retval, *login, table, auths, start, startInclusive, stop, stopInclusive);
      PROTECT(result = Rf_allocVector(STRSXP,1));
      SET_STRING_ELT(result,0, Rf_mkChar(retval.c_str()));
      UNPROTECT(1);
    }
    catch( TException &tx) {
      Rf_error("raccumulo<%s>:: %s",__FUNCTION__,tx.what());
    }
    return(result);
  } 

  /**
   *  Converts a string to the corresponding value in the
   *  SystemPermission enumeration.
   *
   *  Params:
   *    perm = the string to convert
   *
   *  Returns:  the SystemPermission::type enumerated value
   */
  SystemPermission::type getSysPerm(const std::string &perm)
  {
    SystemPermission::type retval = (SystemPermission::type)0;
    int size = _SystemPermission_VALUES_TO_NAMES.size();
    for (int i = 0; i < size; i++)
    {
      std::string val(_SystemPermission_VALUES_TO_NAMES[i]);
      if (val.compare(perm) == 0)
      {
        retval = (SystemPermission::type)i;
        break;
      }
    }
    return retval;
  } 

  /**
   *  Converts a string to the corresponding value in the
   *  PartialKey enumeration.
   *
   *  Params:
   *    perm = the string to convert
   *
   *  Returns:  the SystemPermission::type enumerated value
   */
  PartialKey::type getPartialKey(const std::string &perm)
  {
    PartialKey::type retval = (PartialKey::type)0;
    int size = _PartialKey_VALUES_TO_NAMES.size();
    for (int i = 0; i < size; i++)
    {
      std::string val(_PartialKey_VALUES_TO_NAMES[i]);
      if (val.compare(perm) == 0)
      {
        retval = (PartialKey::type)i;
        break;
      }
    }
    return retval;
  } 

  /**
   *  Converts a string to the corresponding value in the
   *  TablePermission enumeration.
   *
   *  Params:
   *    perm = the string to convert
   *
   *  Returns:  the TablePermission::type enumerated value
   */
  TablePermission::type getTablePerm(const std::string &perm)
  {
    TablePermission::type retval = (TablePermission::type)2;
    int size = _TablePermission_VALUES_TO_NAMES.size();
    int offset = 2; // Values in this enum start at 2 for some reason.
    for (int i = 0; i < size; i++)
    {
      std::string val(_TablePermission_VALUES_TO_NAMES[i + offset]);
      if (val.compare(perm) == 0)
      {
        retval = (TablePermission::type)(i + offset);
        break;
      }
    }
    return retval;
  } 

  /**
   *  Converts a string to the corresponding value in the
   *  IteratorScope enumeration.
   *
   *  Params:
   *    scope = the string to convert
   *
   *  Returns:  the IteratorScope::type enumerated value
   */
  IteratorScope::type getIteratorScope(const std::string &scope)
  {
    IteratorScope::type retval = (IteratorScope::type)0;
    int size = _TablePermission_VALUES_TO_NAMES.size();
    for (int i = 0; i < size; i++)
    {
      std::string val(_IteratorScope_VALUES_TO_NAMES[i]);
      if (val.compare(scope) == 0)
      {
        retval = (IteratorScope::type)(i);
        break;
      }
    }
    return retval;
  } 
 
  /**
   *  Gets a named element out of a list.
   *
   *  Parameters:
   *  list = pointer to the list to search
   *  str = the element name to search for.
   *
   *  Returns: a SEXP to the element, or R_NilValue if not found
   */
  inline SEXP getListElement(SEXP list, const char *str)
  {
    SEXP elmt = R_NilValue, names = getAttrib(list, R_NamesSymbol);
    int i;
    for (i = 0; i < length(list); i++)
         if(strcmp(CHAR(STRING_ELT(names, i)), str) == 0) {
           elmt = VECTOR_ELT(list, i);
           break;
         }
    return elmt;
  }

  /**
   *
   *
   *
   */
  void getRange(SEXP lp, Range& retval)
  {
    SEXP start = getListElement(lp,"start");
    if (start)
    {
      getKey(start,retval.start);
      retval.__isset.start = true;
    }
    bool si = LOGICAL(getListElement(lp,"startInclusive"))[0];
    if (si)
    {
      retval.startInclusive = si;
      retval.__isset.startInclusive = true;
    }
    SEXP end = getListElement(lp,"stop");
    if (end)
    {
      getKey(end,retval.stop);
      retval.__isset.stop = true;
    }
    bool ei = LOGICAL(getListElement(lp,"stopInclusive"))[0];
    if (ei)
    {
      retval.stopInclusive = si;
      retval.__isset.stopInclusive = true;
    }
  }

  /**
   *
   *
   *
   */
  SEXP getRangeAsList(Range range)
  {
    SEXP result = R_NilValue;
    SEXP names = R_NilValue;

    PROTECT(names = Rf_allocVector(STRSXP, 4));
    PROTECT(result = Rf_allocVector(VECSXP, 4));
    SET_STRING_ELT(names, 0, Rf_mkChar("start"));
    SET_VECTOR_ELT(result,0, getKeyAsList(range.start));

    SET_STRING_ELT(names, 1, Rf_mkChar("startInclusive"));
    SET_VECTOR_ELT(result,1,Rf_ScalarLogical(range.startInclusive));

    SET_STRING_ELT(names, 2, Rf_mkChar("stop"));
    SET_VECTOR_ELT(result,2,getKeyAsList(range.stop));

    SET_STRING_ELT(names, 3, Rf_mkChar("stopInclusive"));
    SET_VECTOR_ELT(result,3,Rf_ScalarLogical(range.stopInclusive));

    setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(2);

    return result; 
  }

 /**
   *
   *
   *
   */
  void getKey(SEXP lp, Key& retval)
  {
    std::string row(CHAR(STRING_ELT(getListElement(lp, "row"),0)));
    retval.row = row;
    retval.__isset.row = true;

    std::string fam(CHAR(STRING_ELT(getListElement(lp, "colFamily"),0)));
    if (!fam.empty())
    {
      retval.colFamily = fam;
      retval.__isset.colFamily = true;
    }

    std::string qual(CHAR(STRING_ELT(getListElement(lp, "colQualifier"),0)));
    if (!qual.empty())
    {
      retval.colQualifier = qual;
      retval.__isset.colQualifier = true;
    }

    std::string vis(CHAR(STRING_ELT(getListElement(lp, "colVisibility"),0)));
    if (!vis.empty())
    {
      retval.colVisibility = vis;
      retval.__isset.colVisibility = true;
    }

    int64_t tstamp = INTEGER(getListElement(lp,"timestamp"))[0];
    if (tstamp)
    {
      retval.timestamp = tstamp;
      retval.__isset.timestamp = true;
    }
  }

  /**
   *
   *
   *
   */
  SEXP getKeyAsList(Key key)
  {
    SEXP result = R_NilValue;
    SEXP names = R_NilValue;

    PROTECT(names = Rf_allocVector(STRSXP, 5));
    PROTECT(result = Rf_allocVector(VECSXP, 5));
    SET_STRING_ELT(names, 0, Rf_mkChar("row"));
    SET_VECTOR_ELT(result,0,Rf_ScalarString(Rf_mkChar(key.row.c_str())));

    SET_STRING_ELT(names, 1, Rf_mkChar("colFamily"));
    SET_VECTOR_ELT(result,1,Rf_ScalarString(Rf_mkChar(key.colFamily.c_str())));

    SET_STRING_ELT(names, 2, Rf_mkChar("colQualifier"));
    SET_VECTOR_ELT(result,2,Rf_ScalarString(Rf_mkChar(key.colQualifier.c_str())));

    SET_STRING_ELT(names, 3, Rf_mkChar("colVisibility"));
    SET_VECTOR_ELT(result,3,Rf_ScalarString(Rf_mkChar(key.colVisibility.c_str())));

    SET_STRING_ELT(names, 4, Rf_mkChar("timestamp"));
    SET_VECTOR_ELT(result,4,Rf_ScalarInteger(key.timestamp));

    setAttrib(result, R_NamesSymbol, names);
    UNPROTECT(2);

    return result; 
  }

  /**
   *
   *
   *
   */
  boost::shared_ptr<IteratorSetting> getIteratorSetting(SEXP lp)
  {
    boost::shared_ptr<IteratorSetting> retval(new IteratorSetting());
   
    retval->__set_name(CHAR(STRING_ELT(getListElement(lp, "name"),0)));
    retval->__set_iteratorClass(CHAR(STRING_ELT(getListElement(lp, "class"),0)));
    int32_t priority = INTEGER(getListElement(lp,"priority"))[0];
    if (priority) retval->__set_priority(priority);
    SEXP props = getListElement(lp,"properties");
    if (props && LENGTH(props))
    {
      SEXP names = getAttrib(props, R_NamesSymbol);
      for (int i = 0; i < LENGTH(props); i++)
      {
        std::string name(CHAR(STRING_ELT(names,i)));
        std::string value(CHAR(STRING_ELT(props,i)));
        retval->properties[name] = value; 
      }
    }

    return retval;
  }

 /**
   *
   *
   *
   */
  void getScanColumn(SEXP lp, ScanColumn& retval)
  {
    std::string fam(CHAR(STRING_ELT(getListElement(lp, "colFamily"),0)));
    if (!fam.empty())
    {
      retval.colFamily = fam;
      retval.__isset.colFamily = true;
    }
    std::string qual(CHAR(STRING_ELT(getListElement(lp, "colQualifier"),0)));
    if (!qual.empty())
    {
      retval.colQualifier = qual;
      retval.__isset.colQualifier = true;
    }
  }

 /**
   *
   *
   *
   */
  void getScanOptions(SEXP lp, ScanOptions& retval)
  {
    SEXP auths = getListElement(lp, "authorizations");
    if (auths && LENGTH(auths))
    {
      for (int i = 0; i < LENGTH(auths); i++)
      {
        retval.authorizations.insert(std::string(CHAR(STRING_ELT(auths,i))));
      }
      retval.__isset.authorizations = true;
    }
    SEXP range = getListElement(lp, "range");
    if (range && LENGTH(range))
    {
      getRange(range,retval.range);
      retval.__isset.range = true;
    }
    SEXP scanColumns = getListElement(lp, "columns");
    if (scanColumns && LENGTH(scanColumns))
    {
      for (int i = 0; i < LENGTH(scanColumns); i++)
      {
        SEXP col = VECTOR_ELT(scanColumns, i);
        ScanColumn sc;
        getScanColumn(col,sc);
        retval.columns.push_back(sc);
      }
      retval.__isset.columns = true;
    }
    int32_t bufsiz = INTEGER(getListElement(lp,"bufferSize"))[0];
    if (bufsiz)
    {
      retval.bufferSize = bufsiz;
      retval.__isset.bufferSize = true;
    }
  }

  /**
   *
   *
   *
   */
  void getBatchScanOptions(SEXP lp, BatchScanOptions& retval)
  {
    SEXP auths = getListElement(lp, "authorizations");
    if (auths && LENGTH(auths))
    {
      for (int i = 0; i < LENGTH(auths); i++)
      {
        retval.authorizations.insert(std::string(CHAR(STRING_ELT(auths,i))));
      }
      retval.__isset.authorizations = true;
    }
    SEXP ranges = getListElement(lp, "ranges");
    if (ranges && LENGTH(ranges))
    {
      for (int i = 0; i < LENGTH(ranges); i++)
      {
        SEXP range = VECTOR_ELT(ranges,i);
        Range r;
        getRange(range,r);
        retval.ranges.push_back(r);
      }
      retval.__isset.ranges = true;
    }
    SEXP scanColumns = getListElement(lp, "columns");
    if (scanColumns && LENGTH(scanColumns))
    {
      for (int i = 0; i < LENGTH(scanColumns); i++)
      {
        SEXP col = VECTOR_ELT(scanColumns, i);
        ScanColumn sc;
        getScanColumn(col,sc);
        retval.columns.push_back(sc);
      }
      retval.__isset.columns = true;
    }
    int32_t threads = INTEGER(getListElement(lp,"threads"))[0];
    if (threads)
    {
      retval.threads = threads;
      retval.__isset.threads = true;
    }

    return retval;
  }

} //End extern "C"

