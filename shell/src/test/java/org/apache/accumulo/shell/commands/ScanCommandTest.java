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
package org.apache.accumulo.shell.commands;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ScanCommandTest {

  @Test
  public void testBeginRowHelp() {
    assertTrue("-b should say it is inclusive",
        new ScanCommand().getOptions().getOption("b").getDescription().contains("row (inclusive)"));
  }

  @Test
  public void testCFRowHelp() {
    assertTrue("Column Family",
        new ScanCommand().getOptions().getOption("cf").getDescription().contains("Family"));
  }

  @Test
  public void testCQHelp() {
    assertTrue("Column Qualifier",
        new ScanCommand().getOptions().getOption("cq").getDescription().contains("Qualifier"));
  }

}
