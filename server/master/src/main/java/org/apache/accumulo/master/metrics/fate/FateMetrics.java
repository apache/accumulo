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
package org.apache.accumulo.master.metrics.fate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides basic implementation of fate metrics to provide:
 * <ul>
 * <li>gauge - current count of FATE transactions in progress</li>
 * <li>counter - number of zookeeper node operations on fate root path, provide
 * estimate of fate transaction liveliness</li>
 * <li>counter - the number of zookeeper connection errors since process started.</li>
 * </ul>
 */
public class FateMetrics implements Metrics, FateMetricsMBean {

  // limit calls to update fate counters to guard against hammering zookeeper.
  private long MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(30);

  private AtomicReference<FateMetricValues> metricValues;

  private volatile long lastUpdate = 0;

  private final Master master;

  AdminUtil<String> admin = new AdminUtil<>(false);

  public FateMetrics(final Master master){
    this.master = master;

    metricValues.set(FateMetricValues.Builder.getBuilder().build());

    try {

      Instance instance = master.getInstance();
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();

      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zoo);

    }catch(KeeperException ex){

    } catch(InterruptedException ex){
      Thread.currentThread().interrupt();
    }

  }

  @Override public void register() throws Exception {

  }

  @Override public void add(String name, long time) {

  }

  synchronized void snapshot(){

    long now = System.currentTimeMillis();

    if(now > (lastUpdate + MIN_REFRESH_DELAY)){

      lastUpdate =  now;
    }
  }
  @Override public boolean isEnabled() {
    return false;
  }

  @Override public long currentFateOps() {
    snapshot();
    return metricValues.get().getCurrentFateOps();
  }

  @Override public long fateOpsTotal() {
    snapshot();
    return metricValues.get().getFateOpsTotal();
  }

  @Override public long zkConnectionErrorsTotal() {
    snapshot();
    return metricValues.get().getZkConnectionErrors();
  }


  /**
   * Immutable class that holds a snapshot of fate metric values - use builder to
   * instantiate instance.
   */
  private static class FateMetricValues {

    private final long currentFateOps;
    private final long fateOpsTotal;
    private final long zkConnectionErrors;

    private FateMetricValues(final long currentFateOps, final long fateOpsTotal,
        final long zkConnectionErrors){
      this.currentFateOps = currentFateOps;
      this.fateOpsTotal = fateOpsTotal;
      this.zkConnectionErrors = zkConnectionErrors;
    }

    public long getCurrentFateOps() {
      return currentFateOps;
    }

    public long getFateOpsTotal() {
      return fateOpsTotal;
    }

    public long getZkConnectionErrors() {
      return zkConnectionErrors;
    }

    public static class Builder {

      private long currentFateOps = 0;
      private long fateOpsTotal = 0;
      private long zkConnectionErrors = 0;

      public Builder(){
      }

      public static Builder getBuilder(){
        return new Builder();
      }
      public Builder currentFateOp(final long value){
        this.currentFateOps = value;
        return this;
      }

      public Builder fateOpsTotal(final long value){
        this.fateOpsTotal = value;
        return this;
      }

      public Builder zkConnectionErrors(final long value){
        this.zkConnectionErrors = value;
        return this;
      }

      public FateMetricValues build(){
        return new FateMetricValues(currentFateOps,fateOpsTotal,zkConnectionErrors);
      }
    }
  }

}
