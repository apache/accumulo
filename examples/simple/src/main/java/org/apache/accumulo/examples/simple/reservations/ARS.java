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
package org.apache.accumulo.examples.simple.reservations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * Accumulo Reservation System : An example reservation system using Accumulo. Supports atomic reservations of a resource at a date. Wait list are also
 * supported. In order to keep the example simple, no checking is done of the date. Also the code is inefficient, if interested in improving it take a look at
 * the EXCERCISE comments.
 */

// EXCERCISE create a test that verifies correctness under concurrency. For example, have M threads making reservations against N resources. Each thread could
// randomly reserve and cancel resources for a single user. When each thread finishes, it knows what the state of its single user should be. When all threads
// finish, collect their expected state and verify the status of all users and resources. For extra credit run the test on a IAAS provider using 10 nodes and
// 10 threads per node.

public class ARS {

  private Connector conn;
  private String rTable;

  public enum ReservationResult {
    RESERVED, WAIT_LISTED
  }

  public ARS(Connector conn, String rTable) {
    this.conn = conn;
    this.rTable = rTable;
  }

  public List<String> setCapacity(String what, String when, int count) {
    // EXCERCISE implement this method which atomically sets a capacity and returns anyone who was moved to the wait list if the capacity was decreased

    throw new UnsupportedOperationException();
  }

  public ReservationResult reserve(String what, String when, String who) throws Exception {

    String row = what + ":" + when;

    // EXCERCISE This code assumes there is no reservation and tries to create one. If a reservation exist then the update will fail. This is a good strategy
    // when it is expected there are usually no reservations. Could modify the code to scan first.

    // The following mutation requires that the column tx:seq does not exist and will fail if it does.
    ConditionalMutation update = new ConditionalMutation(row, new Condition("tx", "seq"));
    update.put("tx", "seq", "0");
    update.put("res", String.format("%04d", 0), who);

    ReservationResult result = ReservationResult.RESERVED;

    ConditionalWriter cwriter = conn.createConditionalWriter(rTable, new ConditionalWriterConfig());

    try {
      while (true) {
        Status status = cwriter.write(update).getStatus();
        switch (status) {
          case ACCEPTED:
            return result;
          case REJECTED:
          case UNKNOWN:
            // read the row and decide what to do
            break;
          default:
            throw new RuntimeException("Unexpected status " + status);
        }

        // EXCERCISE in the case of many threads trying to reserve a slot, this approach of immediately retrying is inefficient. Exponential back-off is good
        // general solution to solve contention problems like this. However in this particular case, exponential back-off could penalize the earliest threads
        // that attempted to make a reservation by putting them later in the list. A more complex solution could involve having independent sub-queues within
        // the row that approximately maintain arrival order and use exponential back off to fairly merge the sub-queues into the main queue.

        // it is important to use an isolated scanner so that only whole mutations are seen
        Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
        scanner.setRange(new Range(row));

        int seq = -1;
        int maxReservation = -1;

        for (Entry<Key,Value> entry : scanner) {
          String cf = entry.getKey().getColumnFamilyData().toString();
          String cq = entry.getKey().getColumnQualifierData().toString();
          String val = entry.getValue().toString();

          if (cf.equals("tx") && cq.equals("seq")) {
            seq = Integer.parseInt(val);
          } else if (cf.equals("res")) {
            // EXCERCISE scanning the entire list to find if reserver is already in the list is inefficient. One possible way to solve this would be to sort the
            // data differently in Accumulo so that finding the reserver could be done quickly.
            if (val.equals(who))
              if (maxReservation == -1)
                return ReservationResult.RESERVED; // already have the first reservation
              else
                return ReservationResult.WAIT_LISTED; // already on wait list

            // EXCERCISE the way this code finds the max reservation is very inefficient.... it would be better if it did not have to scan the entire row.
            // One possibility is to just use the sequence number. Could also consider sorting the data in another way and/or using an iterator.
            maxReservation = Integer.parseInt(cq);
          }
        }

        Condition condition = new Condition("tx", "seq");
        if (seq >= 0)
          condition.setValue(seq + ""); // only expect a seq # if one was seen

        update = new ConditionalMutation(row, condition);
        update.put("tx", "seq", (seq + 1) + "");
        update.put("res", String.format("%04d", maxReservation + 1), who);

        // EXCERCISE if set capacity is implemented, then result should take capacity into account
        if (maxReservation == -1)
          result = ReservationResult.RESERVED; // if successful, will be first reservation
        else
          result = ReservationResult.WAIT_LISTED;
      }
    } finally {
      cwriter.close();
    }

  }

  public void cancel(String what, String when, String who) throws Exception {

    String row = what + ":" + when;

    // Even though this method is only deleting a column, its important to use a conditional writer. By updating the seq # when deleting a reservation, it
    // will cause any concurrent reservations to retry. If this delete were done using a batch writer, then a concurrent reservation could report WAIT_LISTED
    // when it actually got the reservation.

    ConditionalWriter cwriter = conn.createConditionalWriter(rTable, new ConditionalWriterConfig());

    try {
      while (true) {

        // its important to use an isolated scanner so that only whole mutations are seen
        Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
        scanner.setRange(new Range(row));

        int seq = -1;
        String reservation = null;

        for (Entry<Key,Value> entry : scanner) {
          String cf = entry.getKey().getColumnFamilyData().toString();
          String cq = entry.getKey().getColumnQualifierData().toString();
          String val = entry.getValue().toString();

          // EXCERCISE avoid linear scan

          if (cf.equals("tx") && cq.equals("seq")) {
            seq = Integer.parseInt(val);
          } else if (cf.equals("res") && val.equals(who)) {
            reservation = cq;
          }
        }

        if (reservation != null) {
          ConditionalMutation update = new ConditionalMutation(row, new Condition("tx", "seq").setValue(seq + ""));
          update.putDelete("res", reservation);
          update.put("tx", "seq", (seq + 1) + "");

          Status status = cwriter.write(update).getStatus();
          switch (status) {
            case ACCEPTED:
              // successfully canceled reservation
              return;
            case REJECTED:
            case UNKNOWN:
              // retry
              // EXCERCISE exponential back-off could be used here
              break;
            default:
              throw new RuntimeException("Unexpected status " + status);
          }

        } else {
          // not reserved, nothing to do
          break;
        }

      }
    } finally {
      cwriter.close();
    }
  }

  public List<String> list(String what, String when) throws Exception {
    String row = what + ":" + when;

    // its important to use an isolated scanner so that only whole mutations are seen
    Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
    scanner.setRange(new Range(row));
    scanner.fetchColumnFamily(new Text("res"));

    List<String> reservations = new ArrayList<String>();

    for (Entry<Key,Value> entry : scanner) {
      String val = entry.getValue().toString();
      reservations.add(val);
    }

    return reservations;
  }

  public static void main(String[] args) throws Exception {
    final ConsoleReader reader = new ConsoleReader();
    ARS ars = null;

    while (true) {
      String line = reader.readLine(">");
      if (line == null)
        break;

      final String[] tokens = line.split("\\s+");

      if (tokens[0].equals("reserve") && tokens.length >= 4 && ars != null) {
        // start up multiple threads all trying to reserve the same resource, no more than one should succeed

        final ARS fars = ars;
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 3; i < tokens.length; i++) {
          final int whoIndex = i;
          Runnable reservationTask = new Runnable() {
            @Override
            public void run() {
              try {
                reader.println("  " + String.format("%20s", tokens[whoIndex]) + " : " + fars.reserve(tokens[1], tokens[2], tokens[whoIndex]));
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          };

          threads.add(new Thread(reservationTask));
        }

        for (Thread thread : threads)
          thread.start();

        for (Thread thread : threads)
          thread.join();

      } else if (tokens[0].equals("cancel") && tokens.length == 4 && ars != null) {
        ars.cancel(tokens[1], tokens[2], tokens[3]);
      } else if (tokens[0].equals("list") && tokens.length == 3 && ars != null) {
        List<String> reservations = ars.list(tokens[1], tokens[2]);
        if (reservations.size() > 0) {
          reader.println("  Reservation holder : " + reservations.get(0));
          if (reservations.size() > 1)
            reader.println("  Wait list : " + reservations.subList(1, reservations.size()));
        }
      } else if (tokens[0].equals("quit") && tokens.length == 1) {
        break;
      } else if (tokens[0].equals("connect") && tokens.length == 6 && ars == null) {
        ZooKeeperInstance zki = new ZooKeeperInstance(new ClientConfiguration().withInstance(tokens[1]).withZkHosts(tokens[2]));
        Connector conn = zki.getConnector(tokens[3], new PasswordToken(tokens[4]));
        if (conn.tableOperations().exists(tokens[5])) {
          ars = new ARS(conn, tokens[5]);
          reader.println("  connected");
        } else
          reader.println("  No Such Table");
      } else {
        System.out.println("  Commands : ");
        if (ars == null) {
          reader.println("    connect <instance> <zookeepers> <user> <pass> <table>");
        } else {
          reader.println("    reserve <what> <when> <who> {who}");
          reader.println("    cancel <what> <when> <who>");
          reader.println("    list <what> <when>");
        }
      }
    }
  }
}
