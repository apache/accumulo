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
package org.apache.accumulo.monitor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collection;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.rpc.TTimeoutTransport;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperStatus implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperStatus.class);

  private volatile boolean stop = false;

  public static class ZooKeeperState implements Comparable<ZooKeeperState> {
    public final String keeper;
    public final String mode;
    public final int clients;

    public ZooKeeperState(String keeper, String mode, int clients) {
      this.keeper = keeper;
      this.mode = mode;
      this.clients = clients;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(keeper);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || (obj != null && obj instanceof ZooKeeperState && 0 == compareTo((ZooKeeperState) obj));
    }

    @Override
    public int compareTo(ZooKeeperState other) {
      if (this == other) {
        return 0;
      } else if (other == null) {
        return 1;
      } else {
        if (this.keeper == other.keeper) {
          return 0;
        } else if (null == this.keeper) {
          return -1;
        } else if (null == other.keeper) {
          return 1;
        } else {
          return this.keeper.compareTo(other.keeper);
        }
      }
    }
  }

  private static SortedSet<ZooKeeperState> status = new TreeSet<>();

  public static Collection<ZooKeeperState> getZooKeeperStatus() {
    return status;
  }

  @Override
  public void run() {

    while (!stop) {

      TreeSet<ZooKeeperState> update = new TreeSet<>();

      String zookeepers[] = SiteConfiguration.getInstance().get(Property.INSTANCE_ZK_HOST).split(",");
      for (String keeper : zookeepers) {
        int clients = 0;
        String mode = "unknown";

        String[] parts = keeper.split(":");
        TTransport transport = null;
        try {
          HostAndPort addr;
          if (parts.length > 1)
            addr = HostAndPort.fromParts(parts[0], Integer.parseInt(parts[1]));
          else
            addr = HostAndPort.fromParts(parts[0], 2181);

          transport = TTimeoutTransport.create(addr, 10 * 1000l);
          transport.write("stat\n".getBytes(UTF_8), 0, 5);
          StringBuilder response = new StringBuilder();
          try {
            transport.flush();
            byte[] buffer = new byte[1024 * 100];
            int n = 0;
            while ((n = transport.read(buffer, 0, buffer.length)) > 0) {
              response.append(new String(buffer, 0, n, UTF_8));
            }
          } catch (TTransportException ex) {
            // happens at EOF
          }
          for (String line : response.toString().split("\n")) {
            if (line.startsWith(" "))
              clients++;
            if (line.startsWith("Mode"))
              mode = line.split(":")[1];
          }
          update.add(new ZooKeeperState(keeper, mode, clients));
        } catch (Exception ex) {
          log.info("Exception talking to zookeeper " + keeper, ex);
          update.add(new ZooKeeperState(keeper, "Down", -1));
        } finally {
          if (transport != null) {
            try {
              transport.close();
            } catch (Exception ex) {
              log.error("Exception", ex);
            }
          }
        }
      }
      status = update;
      sleepUninterruptibly(5, TimeUnit.SECONDS);
    }
  }

}
