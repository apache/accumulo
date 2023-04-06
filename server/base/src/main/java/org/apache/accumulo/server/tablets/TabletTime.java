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
package org.apache.accumulo.server.tablets;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.util.time.RelativeTime;

public abstract class TabletTime {

  public abstract void useMaxTimeFromWALog(long time);

  public abstract MetadataTime getMetadataTime();

  public abstract MetadataTime getMetadataTime(long time);

  public abstract long setUpdateTimes(List<Mutation> mutations);

  public abstract long getTime();

  public abstract long getAndUpdateTime();

  protected void setSystemTimes(Mutation mutation, long lastCommitTime) {
    ServerMutation m = (ServerMutation) mutation;
    m.setSystemTimestamp(lastCommitTime);
  }

  public static TabletTime getInstance(MetadataTime metadataTime) throws IllegalArgumentException {

    if (metadataTime.getType().equals(TimeType.LOGICAL)) {
      return new LogicalTime(metadataTime.getTime());
    } else if (metadataTime.getType().equals(TimeType.MILLIS)) {
      return new MillisTime(metadataTime.getTime());
    } else {
      throw new IllegalArgumentException("Time type unknown : " + metadataTime);
    }
  }

  public static MetadataTime maxMetadataTime(MetadataTime mv1, MetadataTime mv2) {
    // null value will sort lower
    if (mv1 == null || mv2 == null) {
      return mv1 == null ? (mv2 == null ? null : mv2) : mv1;
    }

    return mv1.compareTo(mv2) < 0 ? mv2 : mv1;
  }

  static class MillisTime extends TabletTime {

    private long lastTime;
    private long lastUpdateTime = 0;

    public MillisTime(long time) {
      this.lastTime = time;
    }

    @Override
    public MetadataTime getMetadataTime() {
      return getMetadataTime(lastTime);
    }

    @Override
    public MetadataTime getMetadataTime(long time) {
      return new MetadataTime(time, TimeType.MILLIS);
    }

    @Override
    public void useMaxTimeFromWALog(long time) {
      if (time > lastTime) {
        lastTime = time;
      }
    }

    @Override
    public long setUpdateTimes(List<Mutation> mutations) {

      long currTime = RelativeTime.currentTimeMillis();

      synchronized (this) {
        if (mutations.isEmpty()) {
          return lastTime;
        }

        currTime = updateTime(currTime);
      }

      for (Mutation mutation : mutations) {
        setSystemTimes(mutation, currTime);
      }

      return currTime;
    }

    private long updateTime(long currTime) {
      if (currTime < lastTime) {
        if (currTime - lastUpdateTime > 0) {
          // not in same millisecond as last call
          // to this method so move ahead slowly
          lastTime++;
        }

        lastUpdateTime = currTime;

        currTime = lastTime;
      } else {
        lastTime = currTime;
      }
      return currTime;
    }

    @Override
    public long getTime() {
      return lastTime;
    }

    @Override
    public long getAndUpdateTime() {
      long currTime = RelativeTime.currentTimeMillis();

      synchronized (this) {
        currTime = updateTime(currTime);
      }

      return currTime;
    }

  }

  static class LogicalTime extends TabletTime {
    AtomicLong nextTime;

    private LogicalTime(Long time) {
      this.nextTime = new AtomicLong(time + 1);
    }

    @Override
    public void useMaxTimeFromWALog(long time) {
      time++;

      if (this.nextTime.get() < time) {
        this.nextTime.set(time);
      }
    }

    @Override
    public MetadataTime getMetadataTime() {
      return getMetadataTime(getTime());
    }

    @Override
    public MetadataTime getMetadataTime(long time) {
      return new MetadataTime(time, TimeType.LOGICAL);
    }

    @Override
    public long setUpdateTimes(List<Mutation> mutations) {
      if (mutations.isEmpty()) {
        return getTime();
      }

      long time = nextTime.getAndAdd(mutations.size());
      for (Mutation mutation : mutations) {
        setSystemTimes(mutation, time++);
      }

      return time - 1;
    }

    @Override
    public long getTime() {
      return nextTime.get() - 1;
    }

    @Override
    public long getAndUpdateTime() {
      return nextTime.getAndIncrement();
    }
  }

}
