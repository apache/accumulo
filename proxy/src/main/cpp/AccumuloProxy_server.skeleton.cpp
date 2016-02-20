/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "AccumuloProxy.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::accumulo;

class AccumuloProxyHandler : virtual public AccumuloProxyIf {
 public:
  AccumuloProxyHandler() {
    // Your initialization goes here
  }

  void login(std::string& _return, const std::string& principal, const std::map<std::string, std::string> & loginProperties) {
    // Your implementation goes here
    printf("login\n");
  }

  int32_t addConstraint(const std::string& login, const std::string& tableName, const std::string& constraintClassName) {
    // Your implementation goes here
    printf("addConstraint\n");
  }

  void addSplits(const std::string& login, const std::string& tableName, const std::set<std::string> & splits) {
    // Your implementation goes here
    printf("addSplits\n");
  }

  void attachIterator(const std::string& login, const std::string& tableName, const IteratorSetting& setting, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("attachIterator\n");
  }

  void checkIteratorConflicts(const std::string& login, const std::string& tableName, const IteratorSetting& setting, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("checkIteratorConflicts\n");
  }

  void clearLocatorCache(const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("clearLocatorCache\n");
  }

  void cloneTable(const std::string& login, const std::string& tableName, const std::string& newTableName, const bool flush, const std::map<std::string, std::string> & propertiesToSet, const std::set<std::string> & propertiesToExclude) {
    // Your implementation goes here
    printf("cloneTable\n");
  }

  void compactTable(const std::string& login, const std::string& tableName, const std::string& startRow, const std::string& endRow, const std::vector<IteratorSetting> & iterators, const bool flush, const bool wait, const CompactionStrategyConfig& compactionStrategy) {
    // Your implementation goes here
    printf("compactTable\n");
  }

  void cancelCompaction(const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("cancelCompaction\n");
  }

  void createTable(const std::string& login, const std::string& tableName, const bool versioningIter, const TimeType::type type) {
    // Your implementation goes here
    printf("createTable\n");
  }

  void deleteTable(const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("deleteTable\n");
  }

  void deleteRows(const std::string& login, const std::string& tableName, const std::string& startRow, const std::string& endRow) {
    // Your implementation goes here
    printf("deleteRows\n");
  }

  void exportTable(const std::string& login, const std::string& tableName, const std::string& exportDir) {
    // Your implementation goes here
    printf("exportTable\n");
  }

  void flushTable(const std::string& login, const std::string& tableName, const std::string& startRow, const std::string& endRow, const bool wait) {
    // Your implementation goes here
    printf("flushTable\n");
  }

  void getDiskUsage(std::vector<DiskUsage> & _return, const std::string& login, const std::set<std::string> & tables) {
    // Your implementation goes here
    printf("getDiskUsage\n");
  }

  void getLocalityGroups(std::map<std::string, std::set<std::string> > & _return, const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("getLocalityGroups\n");
  }

  void getIteratorSetting(IteratorSetting& _return, const std::string& login, const std::string& tableName, const std::string& iteratorName, const IteratorScope::type scope) {
    // Your implementation goes here
    printf("getIteratorSetting\n");
  }

  void getMaxRow(std::string& _return, const std::string& login, const std::string& tableName, const std::set<std::string> & auths, const std::string& startRow, const bool startInclusive, const std::string& endRow, const bool endInclusive) {
    // Your implementation goes here
    printf("getMaxRow\n");
  }

  void getTableProperties(std::map<std::string, std::string> & _return, const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("getTableProperties\n");
  }

  void importDirectory(const std::string& login, const std::string& tableName, const std::string& importDir, const std::string& failureDir, const bool setTime) {
    // Your implementation goes here
    printf("importDirectory\n");
  }

  void importTable(const std::string& login, const std::string& tableName, const std::string& importDir) {
    // Your implementation goes here
    printf("importTable\n");
  }

  void listSplits(std::vector<std::string> & _return, const std::string& login, const std::string& tableName, const int32_t maxSplits) {
    // Your implementation goes here
    printf("listSplits\n");
  }

  void listTables(std::set<std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("listTables\n");
  }

  void listIterators(std::map<std::string, std::set<IteratorScope::type> > & _return, const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("listIterators\n");
  }

  void listConstraints(std::map<std::string, int32_t> & _return, const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("listConstraints\n");
  }

  void mergeTablets(const std::string& login, const std::string& tableName, const std::string& startRow, const std::string& endRow) {
    // Your implementation goes here
    printf("mergeTablets\n");
  }

  void offlineTable(const std::string& login, const std::string& tableName, const bool wait) {
    // Your implementation goes here
    printf("offlineTable\n");
  }

  void onlineTable(const std::string& login, const std::string& tableName, const bool wait) {
    // Your implementation goes here
    printf("onlineTable\n");
  }

  void removeConstraint(const std::string& login, const std::string& tableName, const int32_t constraint) {
    // Your implementation goes here
    printf("removeConstraint\n");
  }

  void removeIterator(const std::string& login, const std::string& tableName, const std::string& iterName, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("removeIterator\n");
  }

  void removeTableProperty(const std::string& login, const std::string& tableName, const std::string& property) {
    // Your implementation goes here
    printf("removeTableProperty\n");
  }

  void renameTable(const std::string& login, const std::string& oldTableName, const std::string& newTableName) {
    // Your implementation goes here
    printf("renameTable\n");
  }

  void setLocalityGroups(const std::string& login, const std::string& tableName, const std::map<std::string, std::set<std::string> > & groups) {
    // Your implementation goes here
    printf("setLocalityGroups\n");
  }

  void setTableProperty(const std::string& login, const std::string& tableName, const std::string& property, const std::string& value) {
    // Your implementation goes here
    printf("setTableProperty\n");
  }

  void splitRangeByTablets(std::set<Range> & _return, const std::string& login, const std::string& tableName, const Range& range, const int32_t maxSplits) {
    // Your implementation goes here
    printf("splitRangeByTablets\n");
  }

  bool tableExists(const std::string& login, const std::string& tableName) {
    // Your implementation goes here
    printf("tableExists\n");
  }

  void tableIdMap(std::map<std::string, std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("tableIdMap\n");
  }

  bool testTableClassLoad(const std::string& login, const std::string& tableName, const std::string& className, const std::string& asTypeName) {
    // Your implementation goes here
    printf("testTableClassLoad\n");
  }

  void pingTabletServer(const std::string& login, const std::string& tserver) {
    // Your implementation goes here
    printf("pingTabletServer\n");
  }

  void getActiveScans(std::vector<ActiveScan> & _return, const std::string& login, const std::string& tserver) {
    // Your implementation goes here
    printf("getActiveScans\n");
  }

  void getActiveCompactions(std::vector<ActiveCompaction> & _return, const std::string& login, const std::string& tserver) {
    // Your implementation goes here
    printf("getActiveCompactions\n");
  }

  void getSiteConfiguration(std::map<std::string, std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("getSiteConfiguration\n");
  }

  void getSystemConfiguration(std::map<std::string, std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("getSystemConfiguration\n");
  }

  void getTabletServers(std::vector<std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("getTabletServers\n");
  }

  void removeProperty(const std::string& login, const std::string& property) {
    // Your implementation goes here
    printf("removeProperty\n");
  }

  void setProperty(const std::string& login, const std::string& property, const std::string& value) {
    // Your implementation goes here
    printf("setProperty\n");
  }

  bool testClassLoad(const std::string& login, const std::string& className, const std::string& asTypeName) {
    // Your implementation goes here
    printf("testClassLoad\n");
  }

  bool authenticateUser(const std::string& login, const std::string& user, const std::map<std::string, std::string> & properties) {
    // Your implementation goes here
    printf("authenticateUser\n");
  }

  void changeUserAuthorizations(const std::string& login, const std::string& user, const std::set<std::string> & authorizations) {
    // Your implementation goes here
    printf("changeUserAuthorizations\n");
  }

  void changeLocalUserPassword(const std::string& login, const std::string& user, const std::string& password) {
    // Your implementation goes here
    printf("changeLocalUserPassword\n");
  }

  void createLocalUser(const std::string& login, const std::string& user, const std::string& password) {
    // Your implementation goes here
    printf("createLocalUser\n");
  }

  void dropLocalUser(const std::string& login, const std::string& user) {
    // Your implementation goes here
    printf("dropLocalUser\n");
  }

  void getUserAuthorizations(std::vector<std::string> & _return, const std::string& login, const std::string& user) {
    // Your implementation goes here
    printf("getUserAuthorizations\n");
  }

  void grantSystemPermission(const std::string& login, const std::string& user, const SystemPermission::type perm) {
    // Your implementation goes here
    printf("grantSystemPermission\n");
  }

  void grantTablePermission(const std::string& login, const std::string& user, const std::string& table, const TablePermission::type perm) {
    // Your implementation goes here
    printf("grantTablePermission\n");
  }

  bool hasSystemPermission(const std::string& login, const std::string& user, const SystemPermission::type perm) {
    // Your implementation goes here
    printf("hasSystemPermission\n");
  }

  bool hasTablePermission(const std::string& login, const std::string& user, const std::string& table, const TablePermission::type perm) {
    // Your implementation goes here
    printf("hasTablePermission\n");
  }

  void listLocalUsers(std::set<std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("listLocalUsers\n");
  }

  void revokeSystemPermission(const std::string& login, const std::string& user, const SystemPermission::type perm) {
    // Your implementation goes here
    printf("revokeSystemPermission\n");
  }

  void revokeTablePermission(const std::string& login, const std::string& user, const std::string& table, const TablePermission::type perm) {
    // Your implementation goes here
    printf("revokeTablePermission\n");
  }

  void createBatchScanner(std::string& _return, const std::string& login, const std::string& tableName, const BatchScanOptions& options) {
    // Your implementation goes here
    printf("createBatchScanner\n");
  }

  void createScanner(std::string& _return, const std::string& login, const std::string& tableName, const ScanOptions& options) {
    // Your implementation goes here
    printf("createScanner\n");
  }

  bool hasNext(const std::string& scanner) {
    // Your implementation goes here
    printf("hasNext\n");
  }

  void nextEntry(KeyValueAndPeek& _return, const std::string& scanner) {
    // Your implementation goes here
    printf("nextEntry\n");
  }

  void nextK(ScanResult& _return, const std::string& scanner, const int32_t k) {
    // Your implementation goes here
    printf("nextK\n");
  }

  void closeScanner(const std::string& scanner) {
    // Your implementation goes here
    printf("closeScanner\n");
  }

  void updateAndFlush(const std::string& login, const std::string& tableName, const std::map<std::string, std::vector<ColumnUpdate> > & cells) {
    // Your implementation goes here
    printf("updateAndFlush\n");
  }

  void createWriter(std::string& _return, const std::string& login, const std::string& tableName, const WriterOptions& opts) {
    // Your implementation goes here
    printf("createWriter\n");
  }

  void update(const std::string& writer, const std::map<std::string, std::vector<ColumnUpdate> > & cells) {
    // Your implementation goes here
    printf("update\n");
  }

  void flush(const std::string& writer) {
    // Your implementation goes here
    printf("flush\n");
  }

  void closeWriter(const std::string& writer) {
    // Your implementation goes here
    printf("closeWriter\n");
  }

  ConditionalStatus::type updateRowConditionally(const std::string& login, const std::string& tableName, const std::string& row, const ConditionalUpdates& updates) {
    // Your implementation goes here
    printf("updateRowConditionally\n");
  }

  void createConditionalWriter(std::string& _return, const std::string& login, const std::string& tableName, const ConditionalWriterOptions& options) {
    // Your implementation goes here
    printf("createConditionalWriter\n");
  }

  void updateRowsConditionally(std::map<std::string, ConditionalStatus::type> & _return, const std::string& conditionalWriter, const std::map<std::string, ConditionalUpdates> & updates) {
    // Your implementation goes here
    printf("updateRowsConditionally\n");
  }

  void closeConditionalWriter(const std::string& conditionalWriter) {
    // Your implementation goes here
    printf("closeConditionalWriter\n");
  }

  void getRowRange(Range& _return, const std::string& row) {
    // Your implementation goes here
    printf("getRowRange\n");
  }

  void getFollowing(Key& _return, const Key& key, const PartialKey::type part) {
    // Your implementation goes here
    printf("getFollowing\n");
  }

  void systemNamespace(std::string& _return) {
    // Your implementation goes here
    printf("systemNamespace\n");
  }

  void defaultNamespace(std::string& _return) {
    // Your implementation goes here
    printf("defaultNamespace\n");
  }

  void listNamespaces(std::vector<std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("listNamespaces\n");
  }

  bool namespaceExists(const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("namespaceExists\n");
  }

  void createNamespace(const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("createNamespace\n");
  }

  void deleteNamespace(const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("deleteNamespace\n");
  }

  void renameNamespace(const std::string& login, const std::string& oldNamespaceName, const std::string& newNamespaceName) {
    // Your implementation goes here
    printf("renameNamespace\n");
  }

  void setNamespaceProperty(const std::string& login, const std::string& namespaceName, const std::string& property, const std::string& value) {
    // Your implementation goes here
    printf("setNamespaceProperty\n");
  }

  void removeNamespaceProperty(const std::string& login, const std::string& namespaceName, const std::string& property) {
    // Your implementation goes here
    printf("removeNamespaceProperty\n");
  }

  void getNamespaceProperties(std::map<std::string, std::string> & _return, const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("getNamespaceProperties\n");
  }

  void namespaceIdMap(std::map<std::string, std::string> & _return, const std::string& login) {
    // Your implementation goes here
    printf("namespaceIdMap\n");
  }

  void attachNamespaceIterator(const std::string& login, const std::string& namespaceName, const IteratorSetting& setting, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("attachNamespaceIterator\n");
  }

  void removeNamespaceIterator(const std::string& login, const std::string& namespaceName, const std::string& name, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("removeNamespaceIterator\n");
  }

  void getNamespaceIteratorSetting(IteratorSetting& _return, const std::string& login, const std::string& namespaceName, const std::string& name, const IteratorScope::type scope) {
    // Your implementation goes here
    printf("getNamespaceIteratorSetting\n");
  }

  void listNamespaceIterators(std::map<std::string, std::set<IteratorScope::type> > & _return, const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("listNamespaceIterators\n");
  }

  void checkNamespaceIteratorConflicts(const std::string& login, const std::string& namespaceName, const IteratorSetting& setting, const std::set<IteratorScope::type> & scopes) {
    // Your implementation goes here
    printf("checkNamespaceIteratorConflicts\n");
  }

  int32_t addNamespaceConstraint(const std::string& login, const std::string& namespaceName, const std::string& constraintClassName) {
    // Your implementation goes here
    printf("addNamespaceConstraint\n");
  }

  void removeNamespaceConstraint(const std::string& login, const std::string& namespaceName, const int32_t id) {
    // Your implementation goes here
    printf("removeNamespaceConstraint\n");
  }

  void listNamespaceConstraints(std::map<std::string, int32_t> & _return, const std::string& login, const std::string& namespaceName) {
    // Your implementation goes here
    printf("listNamespaceConstraints\n");
  }

  bool testNamespaceClassLoad(const std::string& login, const std::string& namespaceName, const std::string& className, const std::string& asTypeName) {
    // Your implementation goes here
    printf("testNamespaceClassLoad\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<AccumuloProxyHandler> handler(new AccumuloProxyHandler());
  shared_ptr<TProcessor> processor(new AccumuloProxyProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

