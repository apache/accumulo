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
package org.apache.accumulo.tserver.scan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.Translators;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.tablet.KVEntry;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.Tablet.LookupResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupTask extends ScanTask<MultiScanResult> {

  private static final Logger log = LoggerFactory.getLogger(LookupTask.class);

  private final long scanID;

  public LookupTask(TabletServer server, long scanID) {
    super(server);
    this.scanID = scanID;
  }

  @Override
  public void run() {
    MultiScanSession session = (MultiScanSession) server.getSession(scanID);
    String oldThreadName = Thread.currentThread().getName();

    try {
      if (isCancelled() || session == null)
        return;

      TableConfiguration acuTableConf = server.getTableConfiguration(session.threadPoolExtent);
      long maxResultsSize = acuTableConf.getAsBytes(Property.TABLE_SCAN_MAXMEM);

      runState.set(ScanRunState.RUNNING);
      Thread.currentThread().setName("Client: " + session.client + " User: " + session.getUser() + " Start: " + session.startTime + " Table: ");

      long bytesAdded = 0;
      long maxScanTime = 4000;

      long startTime = System.currentTimeMillis();

      List<KVEntry> results = new ArrayList<>();
      Map<KeyExtent,List<Range>> failures = new HashMap<>();
      List<KeyExtent> fullScans = new ArrayList<>();
      KeyExtent partScan = null;
      Key partNextKey = null;
      boolean partNextKeyInclusive = false;

      Iterator<Entry<KeyExtent,List<Range>>> iter = session.queries.entrySet().iterator();

      // check the time so that the read ahead thread is not monopolized
      while (iter.hasNext() && bytesAdded < maxResultsSize && (System.currentTimeMillis() - startTime) < maxScanTime) {
        Entry<KeyExtent,List<Range>> entry = iter.next();

        iter.remove();

        // check that tablet server is serving requested tablet
        Tablet tablet = server.getOnlineTablet(entry.getKey());
        if (tablet == null) {
          failures.put(entry.getKey(), entry.getValue());
          continue;
        }
        Thread.currentThread().setName(
            "Client: " + session.client + " User: " + session.getUser() + " Start: " + session.startTime + " Tablet: " + entry.getKey().toString());

        LookupResult lookupResult;
        try {

          // do the following check to avoid a race condition
          // between setting false below and the task being
          // canceled
          if (isCancelled())
            interruptFlag.set(true);

          lookupResult = tablet.lookup(entry.getValue(), session.columnSet, session.auths, results, maxResultsSize - bytesAdded, session.ssiList, session.ssio,
              interruptFlag, session.samplerConfig, session.batchTimeOut, session.context);

          // if the tablet was closed it it possible that the
          // interrupt flag was set.... do not want it set for
          // the next
          // lookup
          interruptFlag.set(false);

        } catch (IOException e) {
          log.warn("lookup failed for tablet " + entry.getKey(), e);
          throw new RuntimeException(e);
        }

        bytesAdded += lookupResult.bytesAdded;

        if (lookupResult.unfinishedRanges.size() > 0) {
          if (lookupResult.closed) {
            failures.put(entry.getKey(), lookupResult.unfinishedRanges);
          } else {
            session.queries.put(entry.getKey(), lookupResult.unfinishedRanges);
            partScan = entry.getKey();
            partNextKey = lookupResult.unfinishedRanges.get(0).getStartKey();
            partNextKeyInclusive = lookupResult.unfinishedRanges.get(0).isStartKeyInclusive();
          }
        } else {
          fullScans.add(entry.getKey());
        }
      }

      long finishTime = System.currentTimeMillis();
      session.totalLookupTime += (finishTime - startTime);
      session.numEntries += results.size();

      // convert everything to thrift before adding result
      List<TKeyValue> retResults = new ArrayList<>();
      for (KVEntry entry : results)
        retResults.add(new TKeyValue(entry.getKey().toThrift(), ByteBuffer.wrap(entry.getValue().get())));
      Map<TKeyExtent,List<TRange>> retFailures = Translator.translate(failures, Translators.KET, new Translator.ListTranslator<>(Translators.RT));
      List<TKeyExtent> retFullScans = Translator.translate(fullScans, Translators.KET);
      TKeyExtent retPartScan = null;
      TKey retPartNextKey = null;
      if (partScan != null) {
        retPartScan = partScan.toThrift();
        retPartNextKey = partNextKey.toThrift();
      }
      // add results to queue
      addResult(new MultiScanResult(retResults, retFailures, retFullScans, retPartScan, retPartNextKey, partNextKeyInclusive, session.queries.size() != 0));
    } catch (IterationInterruptedException iie) {
      if (!isCancelled()) {
        log.warn("Iteration interrupted, when scan not cancelled", iie);
        addResult(iie);
      }
    } catch (SampleNotPresentException e) {
      addResult(e);
    } catch (Throwable e) {
      log.warn("exception while doing multi-scan ", e);
      addResult(e);
    } finally {
      Thread.currentThread().setName(oldThreadName);
      runState.set(ScanRunState.FINISHED);
    }
  }
}
