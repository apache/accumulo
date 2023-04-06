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
package org.apache.accumulo.core.client.admin;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.hadoop.io.Text;

public class FindMax {
  private static void appendZeros(ByteArrayOutputStream baos, int num) {
    for (int i = 0; i < num; i++) {
      baos.write(0);
    }
  }

  private static Text findMidPoint(Text minBS, Text maxBS) {
    ByteArrayOutputStream startOS = new ByteArrayOutputStream();
    startOS.write(0); // add a leading zero so bigint does not think its negative
    startOS.write(minBS.getBytes(), 0, minBS.getLength());

    ByteArrayOutputStream endOS = new ByteArrayOutputStream();
    endOS.write(0);// add a leading zero so bigint does not think its negative
    endOS.write(maxBS.getBytes(), 0, maxBS.getLength());

    // make the numbers of the same magnitude
    if (startOS.size() < endOS.size()) {
      appendZeros(startOS, endOS.size() - startOS.size());
    } else if (endOS.size() < startOS.size()) {
      appendZeros(endOS, startOS.size() - endOS.size());
    }

    BigInteger min = new BigInteger(startOS.toByteArray());
    BigInteger max = new BigInteger(endOS.toByteArray());

    BigInteger mid = max.subtract(min).divide(BigInteger.valueOf(2)).add(min);

    byte[] ba = mid.toByteArray();

    Text ret = new Text();

    if (ba.length == startOS.size()) {
      if (ba[0] != 0) {
        throw new RuntimeException();
      }

      // big int added a zero so it would not be negative, drop it
      ret.set(ba, 1, ba.length - 1);
    } else {
      int expLen = Math.max(minBS.getLength(), maxBS.getLength());
      // big int will drop leading 0x0 bytes
      for (int i = ba.length; i < expLen; i++) {
        ret.append(new byte[] {0}, 0, 1);
      }

      ret.append(ba, 0, ba.length);
    }

    // remove trailing 0x0 bytes
    while (ret.getLength() > 0 && ret.getBytes()[ret.getLength() - 1] == 0
        && ret.compareTo(minBS) > 0) {
      Text t = new Text();
      t.set(ret.getBytes(), 0, ret.getLength() - 1);
      ret = t;
    }

    return ret;
  }

  private static Text _findMax(Scanner scanner, Text start, boolean inclStart, Text end,
      boolean inclEnd) {

    // System.out.printf("findMax(%s, %s, %s, %s)%n", Key.toPrintableString(start.getBytes(), 0,
    // start.getLength(), 1000), inclStart,
    // Key.toPrintableString(end.getBytes(), 0, end.getLength(), 1000), inclEnd);

    int cmp = start.compareTo(end);

    if (cmp >= 0) {
      if (inclStart && inclEnd && cmp == 0) {
        scanner.setRange(new Range(start, true, end, true));
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        if (iter.hasNext()) {
          return iter.next().getKey().getRow();
        }
      }

      return null;
    }

    Text mid = findMidPoint(start, end);
    // System.out.println("mid = :"+Key.toPrintableString(mid.getBytes(), 0, mid.getLength(),
    // 1000)+":");

    scanner.setRange(new Range(mid, mid.equals(start) ? inclStart : true, end, inclEnd));

    Iterator<Entry<Key,Value>> iter = scanner.iterator();

    if (iter.hasNext()) {
      Key next = iter.next().getKey();

      int count = 0;
      while (count < 10 && iter.hasNext()) {
        next = iter.next().getKey();
        count++;
      }

      if (!iter.hasNext()) {
        return next.getRow();
      }

      Text ret = _findMax(scanner, next.followingKey(PartialKey.ROW).getRow(), true, end, inclEnd);
      if (ret == null) {
        return next.getRow();
      } else {
        return ret;
      }
    } else {

      return _findMax(scanner, start, inclStart, mid, mid.equals(start) ? inclStart : false);
    }
  }

  private static Text findInitialEnd(Scanner scanner) {
    Text end = new Text(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});

    scanner.setRange(new Range(end, null));

    while (scanner.iterator().hasNext()) {
      Text t = new Text();
      t.append(end.getBytes(), 0, end.getLength());
      t.append(end.getBytes(), 0, end.getLength());
      end = t;
      scanner.setRange(new Range(end, null));
    }

    return end;
  }

  public static Text findMax(Scanner scanner, Text start, boolean is, Text end, boolean ie)
      throws TableNotFoundException {

    scanner.setBatchSize(12);
    IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, SortedKeyIterator.class);
    scanner.addScanIterator(cfg);

    if (start == null) {
      start = new Text();
      is = true;
    }

    if (end == null) {
      end = findInitialEnd(scanner);
    }

    return _findMax(scanner, start, is, end, ie);
  }
}
