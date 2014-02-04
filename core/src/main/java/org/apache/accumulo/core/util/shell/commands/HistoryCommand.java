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
package org.apache.accumulo.core.util.shell.commands;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class HistoryCommand extends Command {
  
  private Option clearHist;
  
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    
    String home = System.getProperty("HOME");
    if (home == null)
      home = System.getenv("HOME");
    final String histDir = home + "/.accumulo";
    int counter = 0;
    
    if (cl.hasOption(clearHist.getOpt())) {
      PrintWriter out = null;
      try {
        FileOutputStream file = new FileOutputStream(histDir + "/shell_history.txt");
        final BufferedWriter fileWriter = new BufferedWriter(new OutputStreamWriter(file, Constants.UTF8));
        out = new PrintWriter(fileWriter);
      } catch (FileNotFoundException e) { 
        e.printStackTrace();
      } finally {
        // If the file existed, closing the 
        if (null != out) {
          out.close();
        }
      }
    }
    
    else {
      BufferedReader in = null;
      try {
        in = new BufferedReader(new InputStreamReader(new FileInputStream(histDir + "/shell_history.txt"), Constants.UTF8));
        String Line;
        try {
          Line = in.readLine();
          while (Line != null) {
            shellState.getReader().printString(counter + " " + Line + "\n");
            counter++;
            Line = in.readLine();
          }
        } catch (IOException e) {
          
          e.printStackTrace();
        }
      } catch (FileNotFoundException e) {
        
        e.printStackTrace();
      } finally {
        if (null != in) {
          in.close();
        }
      }
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return ("generates a list of commands previously executed");
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
  @Override
  public Options getOptions() {
    final Options o = new Options();
    
    clearHist = new Option("c", "clear", false, "clear history file");
    clearHist.setRequired(false);
    
    o.addOption(clearHist);
    
    return o;
  }
}
