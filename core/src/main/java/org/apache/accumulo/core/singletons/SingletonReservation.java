Skip to content
Search or jump to…

Pull requests
Issues
Marketplace
Explore
 
@Umang228 
Umang228
/
accumulo
forked from apache/accumulo
0
0357
Code
Pull requests
Actions
Projects
Security
Insights
Settings
accumulo/core/src/main/java/org/apache/accumulo/core/singletons/SingletonReservation.java /
@ctubbsii
ctubbsii Standardize license headers (apache#1433)
…
Latest commit 3fd5cad on Nov 16, 2019
 History
 2 contributors
@ctubbsii@keith-turner
72 lines (61 sloc)  2.31 KB
  
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.singletons;

import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see SingletonManager#getClientReservation()
 */
public class SingletonReservation implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SingletonReservation.class);

  // AtomicBoolean so cleaner doesn't need to synchronize to reliably read
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Cleanable cleanable;

  public SingletonReservation() {
    cleanable = CleanerUtil.unclosed(this, AccumuloClient.class, closed, log, null);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      SingletonManager.releaseReservation();
    }
  }

  private static class NoopSingletonReservation extends SingletonReservation {
    NoopSingletonReservation() {
      super();
      super.closed.set(true);
      // deregister the cleaner
      super.cleanable.clean();
    }

  }

  private static final SingletonReservation NOOP = new NoopSingletonReservation();

  /**
   * @return A reservation where the close method is a no-op.
   */
  public static SingletonReservation noop() {
    return NOOP;
  }
}
© 2021 GitHub, Inc.
Terms
Privacy
Security
Status
Docs
Contact GitHub
Pricing
API
Training
Blog
About
Loading complete
