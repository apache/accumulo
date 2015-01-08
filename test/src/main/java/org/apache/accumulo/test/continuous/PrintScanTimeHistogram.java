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
package org.apache.accumulo.test.continuous;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class PrintScanTimeHistogram {

  private static final Logger log = Logger.getLogger(PrintScanTimeHistogram.class);

  public static void main(String[] args) throws Exception {
    Histogram<String> srqHist = new Histogram<String>();
    Histogram<String> fsrHist = new Histogram<String>();

    processFile(System.in, srqHist, fsrHist);

    StringBuilder report = new StringBuilder();
    report.append(String.format("%n *** Single row queries histogram *** %n"));
    srqHist.print(report);
    log.info(report);

    report = new StringBuilder();
    report.append(String.format("%n *** Find start rows histogram *** %n"));
    fsrHist.print(report);
    log.info(report);
  }

  private static void processFile(InputStream ins, Histogram<String> srqHist, Histogram<String> fsrHist) throws FileNotFoundException, IOException {
    String line;
    BufferedReader in = new BufferedReader(new InputStreamReader(ins, UTF_8));

    while ((line = in.readLine()) != null) {

      try {
        String[] tokens = line.split(" ");

        String type = tokens[0];
        if (type.equals("SRQ")) {
          long delta = Long.parseLong(tokens[3]);
          String point = generateHistPoint(delta);
          srqHist.addPoint(point);
        } else if (type.equals("FSR")) {
          long delta = Long.parseLong(tokens[3]);
          String point = generateHistPoint(delta);
          fsrHist.addPoint(point);
        }
      } catch (Exception e) {
        System.err.println("Failed to process line : " + line);
        e.printStackTrace();
      }
    }

    in.close();
  }

  private static String generateHistPoint(long delta) {
    String point;

    if (delta / 1000.0 < .1) {
      point = String.format("%07.2f", delta / 1000.0);
      if (point.equals("0000.10"))
        point = "0000.1x";
    } else if (delta / 1000.0 < 1.0) {
      point = String.format("%06.1fx", delta / 1000.0);
      if (point.equals("0001.0x"))
        point = "0001.xx";
    } else {
      point = String.format("%04.0f.xx", delta / 1000.0);
    }
    return point;
  }

}
