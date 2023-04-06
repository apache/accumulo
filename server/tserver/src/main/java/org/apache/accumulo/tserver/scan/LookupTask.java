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
package org.apache.accumulo.tserver.scan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TKey;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.TabletHostingServer;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.tablet.KVEntry;
import org.apache.accumulo.tserver.tablet.Tablet.LookupResult;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupTask extends ScanTask<MultiScanResult> {

  private static final Logger log = LoggerFactory.getLogger(LookupTask.class);

  private final long scanID;

  public LookupTask(TabletHostingServer server, long scanID) {
    super(server);
    this.scanID = scanID;
  }

  @Override
  public void run() {
    MultiScanSession session = (MultiScanSession) server.getSession(scanID);
    String oldThreadName = Thread.currentThread().getName();

    try {
      if (isCancelled() || session == null) {
        return;
      }

      if (!transitionToRunning()) {
        return;
      }

      TableConfiguration acuTableConf = server.getTableConfiguration(session.threadPoolExtent);
      long maxResultsSize = acuTableConf.getAsBytes(Property.TABLE_SCAN_MAXMEM);

      Thread.currentThread().setName("Client: " + session.client + " User: " + session.getUser()
          + " Start: " + session.startTime + " Table: ");

      long bytesAdded = 0;
      long maxScanTime = 4000;

      long startTime = System.currentTimeMillis();

      List<KVEntry> results = new ArrayList<>();
      Map<KeyExtent,List<Range>> failures = new HashMap<>();
      List<KeyExtent> fullScans = new ArrayList<>();
      LookupResult lookupResult = null;
      KeyExtent partScan = null;
      Key partNextKey = null;
      boolean partNextKeyInclusive = false;

      Iterator<Entry<KeyExtent,List<Range>>> iter = session.queries.entrySet().iterator();

      // check the time so that the read ahead thread is not monopolized
      while (iter.hasNext() && bytesAdded < maxResultsSize
          && (System.currentTimeMillis() - startTime) < maxScanTime) {

        final Entry<KeyExtent,List<Range>> entry = iter.next();
        final KeyExtent extent = entry.getKey();
        final List<Range> ranges = entry.getValue();

        iter.remove();

        // check that tablet server is serving requested tablet
        TabletBase tablet = session.getTabletResolver().getTablet(extent);

        if (tablet == null) {
          failures.put(extent, ranges);
          continue;
        }
        Thread.currentThread().setName("Client: " + session.client + " User: " + session.getUser()
            + " Start: " + session.startTime + " Tablet: " + extent);

        try {

          // do the following check to avoid a race condition between setting false below and the
          // task being canceled
          if (isCancelled()) {
            interruptFlag.set(true);
          }

          // Create new List here to collect the results from this Tablet.lookup() call
          // Ensures that the yield code in Tablet can only compare a yield position
          // to the results from that call
          List<KVEntry> tabletResults = new ArrayList<>();
          lookupResult = tablet.lookup(ranges, tabletResults, session.scanParams,
              maxResultsSize - bytesAdded, interruptFlag);
          // Add results from this Tablet.lookup() to the accumulated results
          results.addAll(tabletResults);

          // if the tablet was closed, it is possible that the interrupt flag was set.... do not
          // want it set for the next lookup
          interruptFlag.set(false);

        } catch (IOException e) {
          log.warn("lookup failed for tablet " + extent, e);
          throw new RuntimeException(e);
        }

        bytesAdded += lookupResult.bytesAdded;

        if (lookupResult.unfinishedRanges.isEmpty()) {
          fullScans.add(extent);
        } else {
          if (lookupResult.closed) {
            failures.put(extent, lookupResult.unfinishedRanges);
          } else {
            partScan = extent;
            partNextKey = lookupResult.unfinishedRanges.get(0).getStartKey();
            partNextKeyInclusive = lookupResult.unfinishedRanges.get(0).isStartKeyInclusive();
            // Stop this LookupTask. The client side will continue the scan and a
            // new LookupTask will be created
            break;
          }
        }
      }
      // add unfinished ranges back to session.queries so that we return more=true
      // when the MultiScanResult is created and returned
      if (partScan != null) {
        session.queries.put(partScan, lookupResult.unfinishedRanges);
      }

      long finishTime = System.currentTimeMillis();
      session.totalLookupTime += (finishTime - startTime);
      session.numEntries += results.size();
      boolean queriesIsEmpty = !session.queries.isEmpty();

      // add results to queue
      MultiScanResult multiScanResult = getMultiScanResult(results, partScan, failures, fullScans,
          partNextKey, partNextKeyInclusive, queriesIsEmpty);
      addResult(multiScanResult);
    } catch (IterationInterruptedException iie) {
      if (!isCancelled()) {
        log.warn("Iteration interrupted, when scan not cancelled", iie);
        addResult(iie);
      }
    } catch (SampleNotPresentException e) {
      addResult(e);
    } catch (Exception e) {
      log.warn("exception while doing multi-scan ", e);
      addResult(e);
    } finally {
      Thread.currentThread().setName(oldThreadName);
      runState.set(ScanRunState.FINISHED);
    }
  }

  private MultiScanResult getMultiScanResult(List<KVEntry> results, KeyExtent partScan,
      Map<KeyExtent,List<Range>> failures, List<KeyExtent> fullScans, Key partNextKey,
      boolean partNextKeyInclusive, boolean queriesIsEmpty) {

    // convert everything to thrift before adding result
    List<TKeyValue> retResults = results.stream().map(
        entry -> new TKeyValue(entry.getKey().toThrift(), ByteBuffer.wrap(entry.getValue().get())))
        .collect(Collectors.toList());

    // @formatter:off
    Map<TKeyExtent,List<TRange>> retFailures = failures.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().toThrift(),
            entry -> entry.getValue().stream().map(Range::toThrift).collect(Collectors.toList())
    ));
    // @formatter:on

    List<TKeyExtent> retFullScans =
        fullScans.stream().map(KeyExtent::toThrift).collect(Collectors.toList());

    TKeyExtent retPartScan = null;
    TKey retPartNextKey = null;
    if (partScan != null) {
      retPartScan = partScan.toThrift();
      retPartNextKey = partNextKey.toThrift();
    }

    return new MultiScanResult(retResults, retFailures, retFullScans, retPartScan, retPartNextKey,
        partNextKeyInclusive, queriesIsEmpty);
  }
}
