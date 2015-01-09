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
package org.apache.accumulo.fate;

import static com.google.common.base.Charsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

//TODO use zoocache? - ACCUMULO-1297
//TODO handle zookeeper being down gracefully - ACCUMULO-1297
//TODO document zookeeper layout - ACCUMULO-1298

public class ZooStore<T> implements TStore<T> {

  private String path;
  private IZooReaderWriter zk;
  private String lastReserved = "";
  private Set<Long> reserved;
  private Map<Long,Long> defered;
  private SecureRandom idgenerator;
  private long statusChangeEvents = 0;
  private int reservationsWaiting = 0;

  private byte[] serialize(Object o) {

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      oos.close();

      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object deserialize(byte ser[]) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(ser);
      ObjectInputStream ois = new ObjectInputStream(bais);
      try {
        return ois.readObject();
      } finally {
        ois.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getTXPath(long tid) {
    return String.format("%s/tx_%016x", path, tid);
  }

  private long parseTid(String txdir) {
    return Long.parseLong(txdir.split("_")[1], 16);
  }

  public ZooStore(String path, IZooReaderWriter zk) throws KeeperException, InterruptedException {

    this.path = path;
    this.zk = zk;
    this.reserved = new HashSet<Long>();
    this.defered = new HashMap<Long,Long>();
    this.idgenerator = new SecureRandom();

    zk.putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  @Override
  public long create() {
    while (true) {
      try {
        // looking at the code for SecureRandom, it appears to be thread safe
        long tid = idgenerator.nextLong() & 0x7fffffffffffffffl;
        zk.putPersistentData(getTXPath(tid), TStatus.NEW.name().getBytes(UTF_8), NodeExistsPolicy.FAIL);
        return tid;
      } catch (NodeExistsException nee) {
        // exist, so just try another random #
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public long reserve() {
    try {
      while (true) {

        long events;
        synchronized (this) {
          events = statusChangeEvents;
        }

        List<String> txdirs = new ArrayList<String>(zk.getChildren(path));
        Collections.sort(txdirs);

        synchronized (this) {
          if (txdirs.size() > 0 && txdirs.get(txdirs.size() - 1).compareTo(lastReserved) <= 0)
            lastReserved = "";
        }

        for (String txdir : txdirs) {
          long tid = parseTid(txdir);

          synchronized (this) {
            // this check makes reserve pick up where it left off, so that it cycles through all as it is repeatedly called.... failing to do so can lead to
            // starvation where fate ops that sort higher and hold a lock are never reserved.
            if (txdir.compareTo(lastReserved) <= 0)
              continue;

            if (defered.containsKey(tid)) {
              if (defered.get(tid) < System.currentTimeMillis())
                defered.remove(tid);
              else
                continue;
            }
            if (!reserved.contains(tid)) {
              reserved.add(tid);
              lastReserved = txdir;
            } else
              continue;
          }

          // have reserved id, status should not change

          try {
            TStatus status = TStatus.valueOf(new String(zk.getData(path + "/" + txdir, null), UTF_8));
            if (status == TStatus.IN_PROGRESS || status == TStatus.FAILED_IN_PROGRESS) {
              return tid;
            } else {
              unreserve(tid);
            }
          } catch (NoNodeException nne) {
            // node deleted after we got the list of children, its ok
            unreserve(tid);
          } catch (Exception e) {
            unreserve(tid);
            throw e;
          }
        }

        synchronized (this) {
          if (events == statusChangeEvents) {
            if (defered.size() > 0) {
              Long minTime = Collections.min(defered.values());
              long waitTime = minTime - System.currentTimeMillis();
              if (waitTime > 0)
                this.wait(Math.min(waitTime, 5000));
            } else
              this.wait(5000);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void reserve(long tid) {
    synchronized (this) {
      reservationsWaiting++;
      try {
        while (reserved.contains(tid))
          try {
            this.wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

        reserved.add(tid);
      } finally {
        reservationsWaiting--;
      }
    }
  }

  private void unreserve(long tid) {
    synchronized (this) {
      if (!reserved.remove(tid))
        throw new IllegalStateException("Tried to unreserve id that was not reserved " + String.format("%016x", tid));

      // do not want this unreserve to unesc wake up threads in reserve()... this leads to infinite loop when tx is stuck in NEW...
      // only do this when something external has called reserve(tid)...
      if (reservationsWaiting > 0)
        this.notifyAll();
    }
  }

  @Override
  public void unreserve(long tid, long deferTime) {

    if (deferTime < 0)
      throw new IllegalArgumentException("deferTime < 0 : " + deferTime);

    synchronized (this) {
      if (!reserved.remove(tid))
        throw new IllegalStateException("Tried to unreserve id that was not reserved " + String.format("%016x", tid));

      if (deferTime > 0)
        defered.put(tid, System.currentTimeMillis() + deferTime);

      this.notifyAll();
    }

  }

  private void verifyReserved(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid))
        throw new IllegalStateException("Tried to operate on unreserved transaction " + String.format("%016x", tid));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Repo<T> top(long tid) {
    verifyReserved(tid);

    while (true) {
      try {
        String txpath = getTXPath(tid);
        String top = findTop(txpath);
        if (top == null)
          return null;

        byte[] ser = zk.getData(txpath + "/" + top, null);
        return (Repo<T>) deserialize(ser);
      } catch (KeeperException.NoNodeException ex) {
        continue;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String findTop(String txpath) throws KeeperException, InterruptedException {
    List<String> ops = zk.getChildren(txpath);

    ops = new ArrayList<String>(ops);

    String max = "";

    for (String child : ops)
      if (child.startsWith("repo_") && child.compareTo(max) > 0)
        max = child;

    if (max.equals(""))
      return null;

    return max;
  }

  @Override
  public void push(long tid, Repo<T> repo) throws StackOverflowException {
    verifyReserved(tid);

    String txpath = getTXPath(tid);
    try {
      String top = findTop(txpath);
      if (top != null && Long.parseLong(top.split("_")[1]) > 100) {
        throw new StackOverflowException("Repo stack size too large");
      }

      zk.putPersistentSequential(txpath + "/repo_", serialize(repo));
    } catch (StackOverflowException soe) {
      throw soe;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pop(long tid) {
    verifyReserved(tid);

    try {
      String txpath = getTXPath(tid);
      String top = findTop(txpath);
      if (top == null)
        throw new IllegalStateException("Tried to pop when empty " + tid);
      zk.recursiveDelete(txpath + "/" + top, NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private TStatus _getStatus(long tid) {
    try {
      return TStatus.valueOf(new String(zk.getData(getTXPath(tid), null), UTF_8));
    } catch (NoNodeException nne) {
      return TStatus.UNKNOWN;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TStatus getStatus(long tid) {
    verifyReserved(tid);
    return _getStatus(tid);
  }

  @Override
  public TStatus waitForStatusChange(long tid, EnumSet<TStatus> expected) {
    while (true) {
      long events;
      synchronized (this) {
        events = statusChangeEvents;
      }

      TStatus status = _getStatus(tid);
      if (expected.contains(status))
        return status;

      synchronized (this) {
        if (events == statusChangeEvents) {
          try {
            this.wait(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  @Override
  public void setStatus(long tid, TStatus status) {
    verifyReserved(tid);

    try {
      zk.putPersistentData(getTXPath(tid), status.name().getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    synchronized (this) {
      statusChangeEvents++;
    }

  }

  @Override
  public void delete(long tid) {
    verifyReserved(tid);

    try {
      zk.recursiveDelete(getTXPath(tid), NodeMissingPolicy.SKIP);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setProperty(long tid, String prop, Serializable so) {
    verifyReserved(tid);

    try {
      if (so instanceof String) {
        zk.putPersistentData(getTXPath(tid) + "/prop_" + prop, ("S " + so).getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
      } else {
        byte[] sera = serialize(so);
        byte[] data = new byte[sera.length + 2];
        System.arraycopy(sera, 0, data, 2, sera.length);
        data[0] = 'O';
        data[1] = ' ';
        zk.putPersistentData(getTXPath(tid) + "/prop_" + prop, data, NodeExistsPolicy.OVERWRITE);
      }
    } catch (Exception e2) {
      throw new RuntimeException(e2);
    }
  }

  @Override
  public Serializable getProperty(long tid, String prop) {
    verifyReserved(tid);

    try {
      byte[] data = zk.getData(getTXPath(tid) + "/prop_" + prop, null);

      if (data[0] == 'O') {
        byte[] sera = new byte[data.length - 2];
        System.arraycopy(data, 2, sera, 0, sera.length);
        return (Serializable) deserialize(sera);
      } else if (data[0] == 'S') {
        return new String(data, 2, data.length - 2, UTF_8);
      } else {
        throw new IllegalStateException("Bad property data " + prop);
      }
    } catch (NoNodeException nne) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Long> list() {
    try {
      ArrayList<Long> l = new ArrayList<Long>();
      List<String> transactions = zk.getChildren(path);
      for (String txid : transactions) {
        l.add(parseTid(txid));
      }
      return l;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
