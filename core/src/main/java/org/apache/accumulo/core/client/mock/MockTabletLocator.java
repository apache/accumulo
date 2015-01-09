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
package org.apache.accumulo.core.client.mock;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.hadoop.io.Text;

/**
 * @deprecated since 1.6.0; not intended for public api and you should not use it.
 */
@Deprecated
public class MockTabletLocator extends org.apache.accumulo.core.client.mock.impl.MockTabletLocator {
  public MockTabletLocator() {}

  public TabletLocation locateTablet(Text row, boolean skipRow, boolean retry, TCredentials credentials) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    return locateTablet(Credentials.fromThrift(credentials), row, skipRow, retry);
  }

  public <T extends Mutation> void binMutations(List<T> mutations, Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures,
      TCredentials credentials) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    binMutations(Credentials.fromThrift(credentials), mutations, binnedMutations, failures);
  }

  public List<Range> binRanges(List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges, TCredentials credentials) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    return binRanges(Credentials.fromThrift(credentials), ranges, binnedRanges);
  }
}
