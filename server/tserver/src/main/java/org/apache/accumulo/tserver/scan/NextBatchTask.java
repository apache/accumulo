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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.server.fs.TooManyFilesException;
import org.apache.accumulo.tserver.TabletHostingServer;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NextBatchTask extends ScanTask<ScanBatch> {

  private static final Logger log = LoggerFactory.getLogger(NextBatchTask.class);

  private final long scanID;

  public NextBatchTask(TabletHostingServer server, long scanID, AtomicBoolean interruptFlag) {
    super(server);
    this.scanID = scanID;
    this.interruptFlag = interruptFlag;

    if (interruptFlag.get()) {
      cancel(true);
    }
  }

  @Override
  public void run() {

    final SingleScanSession scanSession = (SingleScanSession) server.getSession(scanID);
    String oldThreadName = Thread.currentThread().getName();

    try {
      if (isCancelled() || scanSession == null) {
        return;
      }

      if (!transitionToRunning()) {
        return;
      }

      Thread.currentThread()
          .setName("User: " + scanSession.getUser() + " Start: " + scanSession.startTime
              + " Client: " + scanSession.client + " Tablet: " + scanSession.extent);

      TabletBase tablet = scanSession.getTabletResolver().getTablet(scanSession.extent);

      if (tablet == null) {
        addResult(new org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException(
            scanSession.extent.toThrift()));
        return;
      }

      ScanBatch batch = scanSession.scanner.read();

      // there should only be one thing on the queue at a time, so
      // it should be ok to call add()
      // instead of put()... if add() fails because queue is at
      // capacity it means there is code
      // problem somewhere
      addResult(batch);
    } catch (TabletClosedException e) {
      addResult(new org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException(
          scanSession.extent.toThrift()));
    } catch (IterationInterruptedException iie) {
      if (!isCancelled()) {
        log.warn("Iteration interrupted, when scan not cancelled", iie);
        addResult(iie);
      }
    } catch (TooManyFilesException | SampleNotPresentException e) {
      addResult(e);
    } catch (IOException | RuntimeException e) {
      log.warn("exception while scanning tablet {} for {}", scanSession.extent, scanSession.client,
          e);
      addResult(e);
    } finally {
      runState.set(ScanRunState.FINISHED);
      Thread.currentThread().setName(oldThreadName);
    }

  }
}
