#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from thrift.protocol import TCompactProtocol
from thrift.transport import TSocket, TTransport

from proxy import AccumuloProxy
from proxy.ttypes import NamespacePermission, IteratorSetting, IteratorScope, AccumuloException


def main():
    transport = TSocket.TSocket('localhost', 42424)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = AccumuloProxy.Client(protocol)
    transport.open()
    login = client.login('root', {'password': 'password'})

    client.createLocalUser(login, 'user1', 'password1')

    print client.listNamespaces(login)

    # create a namespace and give the user1 all permissions
    print 'creating namespace testing'
    client.createNamespace(login, 'testing')
    assert client.namespaceExists(login, 'testing')
    print client.listNamespaces(login)

    print 'testing namespace renaming'
    client.renameNamespace(login, 'testing', 'testing2')
    assert not client.namespaceExists(login, 'testing')
    assert client.namespaceExists(login, 'testing2')
    client.renameNamespace(login, 'testing2', 'testing')
    assert not client.namespaceExists(login, 'testing2')
    assert client.namespaceExists(login, 'testing')

    print 'granting all namespace permissions to user1'
    for k, v in NamespacePermission._VALUES_TO_NAMES.iteritems():
        client.grantNamespacePermission(login, 'user1', 'testing', k)

    # make sure the last operation worked
    for k, v in NamespacePermission._VALUES_TO_NAMES.iteritems():
        assert client.hasNamespacePermission(login, 'user1', 'testing', k), \
            'user1 does\'nt have namespace permission %s' % v

    print 'default namespace: ' + client.defaultNamespace()
    print 'system namespace: ' + client.systemNamespace()

    # grab the namespace properties
    print 'retrieving namespace properties'
    props = client.getNamespaceProperties(login, 'testing')
    assert props and props['table.compaction.major.ratio'] == '3'

    # update a property and verify it is good
    print 'setting namespace property table.compaction.major.ratio = 4'
    client.setNamespaceProperty(login, 'testing', 'table.compaction.major.ratio', '4')
    props = client.getNamespaceProperties(login, 'testing')
    assert props and props['table.compaction.major.ratio'] == '4'

    print 'retrieving namespace ID map'
    nsids = client.namespaceIdMap(login)
    assert nsids and 'accumulo' in nsids

    print 'attaching debug iterator to namespace testing'
    setting = IteratorSetting(priority=40, name='DebugTheThings',
                              iteratorClass='org.apache.accumulo.core.iterators.DebugIterator', properties={})
    client.attachNamespaceIterator(login, 'testing', setting, [IteratorScope.SCAN])
    setting = client.getNamespaceIteratorSetting(login, 'testing', 'DebugTheThings', IteratorScope.SCAN)
    assert setting and setting.name == 'DebugTheThings'

    # make sure the iterator is in the list
    iters = client.listNamespaceIterators(login, 'testing')
    found = False
    for name, scopes in iters.iteritems():
        if name == 'DebugTheThings':
            found = True
            break
    assert found

    print 'checking for iterator conflicts'

    # this next statment should be fine since we are on a different scope
    client.checkNamespaceIteratorConflicts(login, 'testing', setting, [IteratorScope.MINC])

    # this time it should throw an exception since we have already added the iterator with this scope
    try:
        client.checkNamespaceIteratorConflicts(login, 'testing', setting, [IteratorScope.SCAN, IteratorScope.MINC])
    except AccumuloException:
        pass
    else:
        assert False, 'There should have been a namespace iterator conflict!'

    print 'removing debug iterator from namespace testing'
    client.removeNamespaceIterator(login, 'testing', 'DebugTheThings', [IteratorScope.SCAN])

    # make sure the iterator is NOT in the list anymore
    iters = client.listNamespaceIterators(login, 'testing')
    found = False
    for name, scopes in iters.iteritems():
        if name == 'DebugTheThings':
            found = True
            break
    assert not found

    print 'adding max mutation size namespace constraint'
    constraintid = client.addNamespaceConstraint(login, 'testing',
                                                 'org.apache.accumulo.test.constraints.MaxMutationSize')

    print 'make sure constraint was added'
    constraints = client.listNamespaceConstraints(login, 'testing')
    found = False
    for name, cid in constraints.iteritems():
        if cid == constraintid and name == 'org.apache.accumulo.test.constraints.MaxMutationSize':
            found = True
            break
    assert found

    print 'remove max mutation size namespace constraint'
    client.removeNamespaceConstraint(login, 'testing', constraintid)

    print 'make sure constraint was removed'
    constraints = client.listNamespaceConstraints(login, 'testing')
    found = False
    for name, cid in constraints.iteritems():
        if cid == constraintid and name == 'org.apache.accumulo.test.constraints.MaxMutationSize':
            found = True
            break
    assert not found

    print 'test a namespace class load of the VersioningIterator'
    res = client.testNamespaceClassLoad(login, 'testing', 'org.apache.accumulo.core.iterators.user.VersioningIterator',
                                        'org.apache.accumulo.core.iterators.SortedKeyValueIterator')
    assert res

    print 'test a bad namespace class load of the VersioningIterator'
    res = client.testNamespaceClassLoad(login, 'testing', 'org.apache.accumulo.core.iterators.user.VersioningIterator',
                                        'dummy')
    assert not res

    # revoke the permissions
    print 'revoking namespace permissions for user1'
    for k, v in NamespacePermission._VALUES_TO_NAMES.iteritems():
        client.revokeNamespacePermission(login, 'user1', 'testing', k)

    # make sure the last operation worked
    for k, v in NamespacePermission._VALUES_TO_NAMES.iteritems():
        assert not client.hasNamespacePermission(login, 'user1', 'testing', k), \
            'user1 does\'nt have namespace permission %s' % v

    print 'deleting namespace testing'
    client.deleteNamespace(login, 'testing')
    assert not client.namespaceExists(login, 'testing')

    print 'deleting user1'
    client.dropLocalUser(login, 'user1')

if __name__ == "__main__":
    main()
