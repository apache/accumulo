/**
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
package org.apache.accumulo.core.util.shell.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.mock.MockShell;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Uses the MockShell to test the shell output with Formatters
 */
public class FormatterCommandTest {
  Writer writer = null;
  InputStream in = null;
  
  @Test
  public void test() throws IOException, AccumuloException, AccumuloSecurityException, TableExistsException, ClassNotFoundException {
    // Keep the Shell AUDIT log off the test output
    Logger.getLogger(Shell.class).setLevel(Level.WARN);
    
    String[] args = new String[] {"-fake", "-u", "root", "-p", "passwd"};
   
    String[] commands = createCommands();
    
    in = MockShell.makeCommands(commands);
    writer = new StringWriter();
    
    MockShell shell = new MockShell(in, writer);
    shell.config(args);
    
    // Can't call createtable in the shell with MockAccumulo
    shell.getConnector().tableOperations().create("test");

    try {
      shell.start();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      Assert.fail("Exception while running commands: " + e.getMessage());
    } 
    
    shell.getReader().flushConsole();
    
    String[] output = StringUtils.split(writer.toString(), '\n');
   
    boolean formatterOn = false;
    
    String[] expectedDefault = new String[] {
        "row cf:cq []    1234abcd",
        "row cf1:cq1 []    9876fedc",
        "row2 cf:cq []    13579bdf",
        "row2 cf1:cq []    2468ace"
    };
    
    String[] expectedFormatted = new String[] {
        "row cf:cq []    0x31 0x32 0x33 0x34 0x61 0x62 0x63 0x64",
        "row cf1:cq1 []    0x39 0x38 0x37 0x36 0x66 0x65 0x64 0x63",
        "row2 cf:cq []    0x31 0x33 0x35 0x37 0x39 0x62 0x64 0x66",
        "row2 cf1:cq []    0x32 0x34 0x36 0x38 0x61 0x63 0x65"
    };
    
    int outputIndex = 0;
    while (outputIndex < output.length) {
      String line = output[outputIndex];
      
      if (line.startsWith("root@mock-instance")) {
        if (line.contains("formatter -t test -f org.apache.accumulo.core.util.shell.command.FormatterCommandTest$HexFormatter")) {
          formatterOn = true;
        }
       
        outputIndex++;
      } else if (line.startsWith("row")) {
        int expectedIndex = 0;
        String[] comparisonData;
        
        // Pick the type of data we expect (formatted or default)
        if (formatterOn) {
          comparisonData = expectedFormatted;
        } else {
          comparisonData = expectedDefault;
        }
        
        // Ensure each output is what we expected
        while (expectedIndex + outputIndex < output.length &&
            expectedIndex < expectedFormatted.length) {
          Assert.assertEquals(comparisonData[expectedIndex].trim(), output[expectedIndex + outputIndex].trim());
          
          expectedIndex++;
        }
        
        outputIndex += expectedIndex;
      }
    }
  }
  
  private String[] createCommands() {
    return new String[] {
        "table test",
        "insert row cf cq 1234abcd",
        "insert row cf1 cq1 9876fedc",
        "insert row2 cf cq 13579bdf",
        "insert row2 cf1 cq 2468ace",
        "scan",
        "formatter -t test -f org.apache.accumulo.core.util.shell.command.FormatterCommandTest$HexFormatter",
        "scan"
    };
  }
  
  /**
   * <p>Simple <code>Formatter</code> that will convert each character in the Value
   * from decimal to hexadecimal. Will automatically skip over characters in the value
   * which do not fall within the [0-9,a-f] range.</p>
   * 
   * <p>Example: <code>'0'</code> will be displayed as <code>'0x30'</code></p>
   */
  public static class HexFormatter implements Formatter {
    private Iterator<Entry<Key, Value>> iter = null;
    private boolean printTs = false;

    private final String tab = "\t";
    private final String newline = "\n";
    
    public HexFormatter() {}
    
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      return this.iter.hasNext();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public String next() {
      Entry<Key, Value> entry = iter.next();
      
      String key;
      
      // Observe the timestamps
      if (printTs) {
        key = entry.getKey().toString();
      } else {
        key = entry.getKey().toStringNoTime();
      }
      
      Value v = entry.getValue();
      
      // Approximate how much space we'll need
      StringBuilder sb = new StringBuilder(key.length() + v.getSize() * 5); 
      
      sb.append(key).append(tab);
      
      for (byte b : v.get()) {
        if ((b >= 48 && b <= 57) || (b >= 97 || b <= 102)) {
          sb.append(String.format("0x%x ", new Integer(b)));
        }
      }
      
      sb.append(newline);
      
      return sb.toString();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
      return;
    }

    /* (non-Javadoc)
     * @see org.apache.accumulo.core.util.format.Formatter#initialize(java.lang.Iterable, boolean)
     */
    @Override
    public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
      this.iter = scanner.iterator();
      this.printTs = printTimestamps;
    }
    
  }
  
}
