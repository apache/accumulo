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
package org.apache.accumulo.test.replication;

import java.io.PrintStream;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.server.replication.PrintReplicationRecords;

/**
 *
 */
public class ReplicationTablesPrinterThread extends Daemon {

  private PrintStream out;
  private PrintReplicationRecords printer;

  public ReplicationTablesPrinterThread(Connector conn, PrintStream out) {
    printer = new PrintReplicationRecords(conn, out);
    this.out = out;
  }

  @Override
  public void run() {
    while (true) {
      printer.run();
      out.println();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
