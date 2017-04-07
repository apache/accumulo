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
package org.apache.accumulo.server.tablets;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.util.time.RelativeTime;

public abstract class TabletTime {
  public static final char LOGICAL_TIME_ID = 'L';
  public static final char MILLIS_TIME_ID = 'M';

  public static char getTimeID(TimeType timeType) {
    switch (timeType) {
      case LOGICAL:
        return LOGICAL_TIME_ID;
      case MILLIS:
        return MILLIS_TIME_ID;
    }

    throw new IllegalArgumentException("Unknown time type " + timeType);
  }

  public abstract void useMaxTimeFromWALog(long time);

  public abstract String getMetadataValue(long time);

  public abstract String getMetadataValue();

  public abstract long setUpdateTimes(List<Mutation> mutations);

  public abstract long getTime();

  public abstract long getAndUpdateTime();

  protected void setSystemTimes(Mutation mutation, long lastCommitTime) {
    ServerMutation m = (ServerMutation) mutation;
    m.setSystemTimestamp(lastCommitTime);
  }

  public static TabletTime getInstance(String metadataValue) {
    if (metadataValue.charAt(0) == LOGICAL_TIME_ID) {
      return new LogicalTime(Long.parseLong(metadataValue.substring(1)));
    } else if (metadataValue.charAt(0) == MILLIS_TIME_ID) {
      return new MillisTime(Long.parseLong(metadataValue.substring(1)));
    }

    throw new IllegalArgumentException("Time type unknown : " + metadataValue);

  }

  public static String maxMetadataTime(String mv1, String mv2) {
    if (mv1 == null && mv2 == null) {
      return null;
    }

    if (mv1 == null) {
      checkType(mv2);
      return mv2;
    }

    if (mv2 == null) {
      checkType(mv1);
      return mv1;
    }

    if (mv1.charAt(0) != mv2.charAt(0))
      throw new IllegalArgumentException("Time types differ " + mv1 + " " + mv2);
    checkType(mv1);

    long t1 = Long.parseLong(mv1.substring(1));
    long t2 = Long.parseLong(mv2.substring(1));

    if (t1 < t2)
      return mv2;
    else
      return mv1;

  }

  private static void checkType(String mv1) {
    if (mv1.charAt(0) != LOGICAL_TIME_ID && mv1.charAt(0) != MILLIS_TIME_ID)
      throw new IllegalArgumentException("Invalid time type " + mv1);
  }

  static class MillisTime extends TabletTime {

    private long lastTime;
    private long lastUpdateTime = 0;

    public MillisTime(long time) {
      this.lastTime = time;
    }

    @Override
    public String getMetadataValue(long time) {
      return MILLIS_TIME_ID + "" + time;
    }

    @Override
    public String getMetadataValue() {
      return getMetadataValue(lastTime);
    }

    @Override
    public void useMaxTimeFromWALog(long time) {
      if (time > lastTime)
        lastTime = time;
    }

    @Override
    public long setUpdateTimes(List<Mutation> mutations) {

      long currTime = RelativeTime.currentTimeMillis();

      synchronized (this) {
        if (mutations.size() == 0)
          return lastTime;

        currTime = updateTime(currTime);
      }

      for (Mutation mutation : mutations)
        setSystemTimes(mutation, currTime);

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
      this.nextTime = new AtomicLong(time.longValue() + 1);
    }

    @Override
    public void useMaxTimeFromWALog(long time) {
      time++;

      if (this.nextTime.get() < time) {
        this.nextTime.set(time);
      }
    }

    @Override
    public String getMetadataValue() {
      return getMetadataValue(getTime());
    }

    @Override
    public String getMetadataValue(long time) {
      return LOGICAL_TIME_ID + "" + time;
    }

    @Override
    public long setUpdateTimes(List<Mutation> mutations) {
      if (mutations.size() == 0)
        return getTime();

      long time = nextTime.getAndAdd(mutations.size());
      for (Mutation mutation : mutations)
        setSystemTimes(mutation, time++);

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
