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
package org.apache.accumulo.core.client.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AccumuloInputFormatTest {

  private JobConf job;

  @Rule
  public TestName test = new TestName();

  @Before
  public void createJob() {
    job = new JobConf();
  }

  /**
   * Check that the iterator configuration is getting stored in the Job conf correctly.
   */
  @Test
  public void testSetIterator() throws IOException {
    IteratorSetting is = new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator");
    AccumuloInputFormat.addIterator(job, is);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    is.write(new DataOutputStream(baos));
    String iterators = job.get("AccumuloInputFormat.ScanOpts.Iterators");
    assertEquals(Base64.getEncoder().encodeToString(baos.toByteArray()), iterators);
  }

  @Test
  public void testAddIterator() throws IOException {
    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", WholeRowIterator.class));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    IteratorSetting iter = new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator");
    iter.addOption("v1", "1");
    iter.addOption("junk", "\0omg:!\\xyzzy");
    AccumuloInputFormat.addIterator(job, iter);

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);

    // Check the list size
    assertTrue(list.size() == 3);

    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.user.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    assertEquals(0, setting.getOptions().size());

    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    assertEquals(0, setting.getOptions().size());

    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
    assertEquals(2, setting.getOptions().size());
    assertEquals("1", setting.getOptions().get("v1"));
    assertEquals("\0omg:!\\xyzzy", setting.getOptions().get("junk"));
  }

  /**
   * Test adding iterator options where the keys and values contain both the FIELD_SEPARATOR character (':') and ITERATOR_SEPARATOR (',') characters. There
   * should be no exceptions thrown when trying to parse these types of option entries.
   *
   * This test makes sure that the expected raw values, as appears in the Job, are equal to what's expected.
   */
  @Test
  public void testIteratorOptionEncoding() throws Throwable {
    String key = "colon:delimited:key";
    String value = "comma,delimited,value";
    IteratorSetting someSetting = new IteratorSetting(1, "iterator", "Iterator.class");
    someSetting.addOption(key, value);
    AccumuloInputFormat.addIterator(job, someSetting);

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);
    assertEquals(1, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);

    someSetting.addOption(key + "2", value);
    someSetting.setPriority(2);
    someSetting.setName("it2");
    AccumuloInputFormat.addIterator(job, someSetting);
    list = AccumuloInputFormat.getIterators(job);
    assertEquals(2, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);
    assertEquals(2, list.get(1).getOptions().size());
    assertEquals(list.get(1).getOptions().get(key), value);
    assertEquals(list.get(1).getOptions().get(key + "2"), value);
  }

  /**
   * Test getting iterator settings for multiple iterators set
   */
  @Test
  public void testGetIteratorSettings() throws IOException {
    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator"));

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);

    // Check the list size
    assertTrue(list.size() == 3);

    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());

    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());

    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());

  }

  @Test
  public void testSetRegex() throws IOException {
    String regex = ">\"*%<>\'\\";

    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    AccumuloInputFormat.addIterator(job, is);

    assertTrue(regex.equals(AccumuloInputFormat.getIterators(job).get(0).getName()));
  }

}
