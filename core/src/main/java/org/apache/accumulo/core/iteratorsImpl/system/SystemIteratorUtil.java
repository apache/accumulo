/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TIteratorSetting;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * System utility class. Not for client use.
 */
public class SystemIteratorUtil {

  public static TIteratorSetting toTIteratorSetting(IteratorSetting is) {
    return new TIteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(),
        is.getOptions());
  }

  public static IteratorSetting toIteratorSetting(TIteratorSetting tis) {
    return new IteratorSetting(tis.getPriority(), tis.getName(), tis.getIteratorClass(),
        tis.getProperties());
  }

  public static IteratorConfig toIteratorConfig(List<IteratorSetting> iterators) {
    ArrayList<TIteratorSetting> tisList = new ArrayList<>();

    for (IteratorSetting iteratorSetting : iterators) {
      tisList.add(toTIteratorSetting(iteratorSetting));
    }

    return new IteratorConfig(tisList);
  }

  public static List<IteratorSetting> toIteratorSettings(IteratorConfig ic) {
    List<IteratorSetting> ret = new ArrayList<>();
    for (TIteratorSetting tIteratorSetting : ic.getIterators()) {
      ret.add(toIteratorSetting(tIteratorSetting));
    }

    return ret;
  }

  public static byte[] encodeIteratorSettings(IteratorConfig iterators) {
    try {
      TSerializer tser = new TSerializer(new TBinaryProtocol.Factory());
      return tser.serialize(iterators);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encodeIteratorSettings(List<IteratorSetting> iterators) {
    return encodeIteratorSettings(toIteratorConfig(iterators));
  }

  public static List<IteratorSetting> decodeIteratorSettings(byte[] enc) {
    IteratorConfig ic = new IteratorConfig();
    try {
      TDeserializer tdser = new TDeserializer(new TBinaryProtocol.Factory());
      tdser.deserialize(ic, enc);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return toIteratorSettings(ic);
  }

  public static SortedKeyValueIterator<Key,Value> setupSystemScanIterators(
      SortedKeyValueIterator<Key,Value> source, Set<Column> cols, Authorizations auths,
      byte[] defaultVisibility, AccumuloConfiguration conf) throws IOException {
    SortedKeyValueIterator<Key,Value> delIter =
        DeletingIterator.wrap(source, false, DeletingIterator.getBehavior(conf));
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);
    SortedKeyValueIterator<Key,Value> colFilter = ColumnQualifierFilter.wrap(cfsi, cols);
    return VisibilityFilter.wrap(colFilter, auths, defaultVisibility);
  }
}
