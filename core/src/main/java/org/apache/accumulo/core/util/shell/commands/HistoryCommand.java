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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class HistoryCommand extends Command {
  
  private Option clearHist;
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    
    String histDir = System.getenv("HOME") + "/.accumulo";
    int counter = 0;
    
    if (cl.hasOption(clearHist.getOpt())) {
      
      try {
        
        FileWriter outFile = new FileWriter(histDir + "/shell_history.txt");
        PrintWriter out = new PrintWriter(outFile);
        out.close();
        
      } catch (IOException e) {
        
        e.printStackTrace();
      }
    }
    
    else {
      try {
        BufferedReader in = new BufferedReader(new FileReader(histDir + "/shell_history.txt"));
        String Line;
        try {
          Line = in.readLine();
          while (Line != null) {
            shellState.getReader().printString(counter + " " + Line);
            shellState.getReader().printNewline();
            counter++;
            Line = in.readLine();
          }
        } catch (IOException e) {
          
          e.printStackTrace();
        }
      } catch (FileNotFoundException e) {
        
        e.printStackTrace();
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
    Options o = new Options();
    
    clearHist = new Option("c", "clear", false, "clear history file");
    clearHist.setRequired(false);
    
    o.addOption(clearHist);
    
    return o;
  }
}
