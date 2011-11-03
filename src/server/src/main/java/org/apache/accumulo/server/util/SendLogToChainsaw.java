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
package org.apache.accumulo.server.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.net.Socket;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.apache.log4j.varia.LevelRangeFilter;
import org.apache.log4j.xml.XMLLayout;

public class SendLogToChainsaw extends XMLLayout {
  
  private static Pattern logPattern = Pattern.compile(
      "^(\\d\\d)\\s(\\d\\d):(\\d\\d):(\\d\\d),(\\d\\d\\d)\\s\\[(.*)\\]\\s(TRACE|DEBUG|INFO|WARN|FATAL|ERROR)\\s*?:(.*)$", Pattern.UNIX_LINES);
  
  private File[] logFiles = null;
  
  private SocketFactory factory = SocketFactory.getDefault();
  
  private WildcardFileFilter fileFilter = null;
  
  private Socket socket = null;
  
  private Pattern lineFilter = null;
  
  private LongRange dateFilter = null;
  
  private LevelRangeFilter levelFilter = null;
  
  public SendLogToChainsaw(String directory, String fileNameFilter, String host, int port, Date start, Date end, String regex, String level) throws Exception {
    
    // Set up the file name filter
    if (null != fileNameFilter) {
      fileFilter = new WildcardFileFilter(fileNameFilter);
    } else {
      fileFilter = new WildcardFileFilter("*");
    }
    
    // Get the list of files that match
    File dir = new File(directory);
    if (dir.isDirectory()) {
      logFiles = dir.listFiles((FilenameFilter) fileFilter);
    } else {
      throw new IllegalArgumentException(directory + " is not a directory or is not readable.");
    }
    
    if (logFiles.length == 0) {
      throw new IllegalArgumentException("No files match the supplied filter.");
    }
    
    socket = factory.createSocket(host, port);
    
    lineFilter = Pattern.compile(regex);
    
    // Create Date Filter
    if (null != start) {
      if (end == null)
        end = new Date(System.currentTimeMillis());
      dateFilter = new LongRange(start.getTime(), end.getTime());
    }
    
    if (null != level) {
      Level base = Level.toLevel(level.toUpperCase());
      levelFilter = new LevelRangeFilter();
      levelFilter.setAcceptOnMatch(true);
      levelFilter.setLevelMin(base);
      levelFilter.setLevelMax(Level.FATAL);
    }
  }
  
  public void processLogFiles() throws Exception {
    for (File log : logFiles) {
      // Parse the server type and name from the log file name
      String threadName = log.getName().substring(0, log.getName().indexOf("."));
      FileReader fReader = new FileReader(log);
      BufferedReader reader = new BufferedReader(fReader);
      
      String line = reader.readLine();
      while (null != line) {
        String out = null;
        try {
          out = convertLine(line, threadName);
          if (null != out) {
            if (socket != null && socket.isConnected())
              socket.getOutputStream().write(out.getBytes());
            else
              System.err.println("Unable to send data to transport");
          }
        } catch (Exception e) {
          System.out.println("Error processing line: " + line + ". Output was " + out);
          throw e;
        }
        line = reader.readLine();
      }
      reader.close();
      fReader.close();
    }
    if (socket != null && socket.isConnected())
      socket.close();
  }
  
  private String convertLine(String line, String threadName) throws Exception {
    String result = null;
    Matcher m = logPattern.matcher(line);
    if (m.matches()) {
      
      Calendar cal = Calendar.getInstance();
      cal.setTime(new Date(System.currentTimeMillis()));
      Integer date = Integer.parseInt(m.group(1));
      Integer hour = Integer.parseInt(m.group(2));
      Integer min = Integer.parseInt(m.group(3));
      Integer sec = Integer.parseInt(m.group(4));
      Integer ms = Integer.parseInt(m.group(5));
      String clazz = m.group(6);
      String level = m.group(7);
      String message = m.group(8);
      // Apply the regex filter if supplied
      if (null != lineFilter) {
        Matcher match = lineFilter.matcher(message);
        if (!match.matches())
          return null;
      }
      // URL encode the message
      message = URLEncoder.encode(message, "UTF-8");
      // Assume that we are processing logs from today.
      // If the date in the line is greater than today, then it must be
      // from the previous month.
      cal.set(Calendar.DATE, date);
      cal.set(Calendar.HOUR_OF_DAY, hour);
      cal.set(Calendar.MINUTE, min);
      cal.set(Calendar.SECOND, sec);
      cal.set(Calendar.MILLISECOND, ms);
      if (date > cal.get(Calendar.DAY_OF_MONTH)) {
        cal.add(Calendar.MONTH, -1);
      }
      long ts = cal.getTimeInMillis();
      // If this event is not between the start and end dates, then skip it.
      if (null != dateFilter && !dateFilter.containsLong(ts))
        return null;
      Category c = Logger.getLogger(clazz);
      Level l = Level.toLevel(level);
      LoggingEvent event = new LoggingEvent(clazz, c, ts, l, message, threadName, (ThrowableInformation) null, (String) null, (LocationInfo) null,
          (Map<?,?>) null);
      // Check the log level filter
      if (null != levelFilter && (levelFilter.decide(event) == Filter.DENY)) {
        return null;
      }
      result = format(event);
    }
    return result;
  }
  
  private static Options getOptions() {
    Options opts = new Options();
    
    Option dirOption = new Option("d", "logDirectory", true, "ACCUMULO log directory path");
    dirOption.setArgName("dir");
    dirOption.setRequired(true);
    opts.addOption(dirOption);
    
    Option fileFilterOption = new Option("f", "fileFilter", true, "filter to apply to names of logs");
    fileFilterOption.setArgName("filter");
    fileFilterOption.setRequired(false);
    opts.addOption(fileFilterOption);
    
    Option hostOption = new Option("h", "host", true, "host where chainsaw is running");
    hostOption.setArgName("hostname");
    hostOption.setRequired(true);
    opts.addOption(hostOption);
    
    Option portOption = new Option("p", "port", true, "port where XMLSocketReceiver is listening");
    portOption.setArgName("portnum");
    portOption.setRequired(true);
    opts.addOption(portOption);
    
    Option startOption = new Option("s", "start", true, "start date filter (yyyyMMddHHmmss)");
    startOption.setArgName("date");
    startOption.setRequired(true);
    opts.addOption(startOption);
    
    Option endOption = new Option("e", "end", true, "end date filter (yyyyMMddHHmmss)");
    endOption.setArgName("date");
    endOption.setRequired(true);
    opts.addOption(endOption);
    
    Option levelOption = new Option("l", "level", true, "filter log level");
    levelOption.setArgName("level");
    levelOption.setRequired(false);
    opts.addOption(levelOption);
    
    Option msgFilter = new Option("m", "messageFilter", true, "regex filter for log messages");
    msgFilter.setArgName("regex");
    msgFilter.setRequired(false);
    opts.addOption(msgFilter);
    
    return opts;
    
  }
  
  /**
   * 
   * @param args
   *          parameter 0: path to log directory parameter 1: filter to apply for logs to include (uses wildcards (i.e. logger* and IS case sensitive) parameter
   *          2: chainsaw host parameter 3: chainsaw port parameter 4: start date filter parameter 5: end date filter parameter 6: optional regex filter to
   *          match on each log4j message parameter 7: optional level filter
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    Options o = getOptions();
    CommandLine cl = null;
    cl = new BasicParser().parse(o, args);
    
    String logDir = cl.getOptionValue(o.getOption("d").getOpt());
    String fileNameFilter = null;
    if (cl.hasOption(o.getOption("f").getOpt()))
      fileNameFilter = cl.getOptionValue(o.getOption("f").getOpt());
    String chainsawHost = cl.getOptionValue(o.getOption("h").getOpt());
    int chainsawPort = 0;
    try {
      chainsawPort = Integer.parseInt(cl.getOptionValue(o.getOption("p").getOpt()));
    } catch (NumberFormatException nfe) {
      System.err.println("Unable to parse port number");
      System.exit(-1);
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    Date startDate = null;
    if (cl.hasOption(o.getOption("s").getOpt())) {
      startDate = formatter.parse(cl.getOptionValue(o.getOption("s").getOpt()));
    }
    Date endDate = null;
    if (cl.hasOption(o.getOption("e").getOpt())) {
      endDate = formatter.parse(cl.getOptionValue(o.getOption("e").getOpt()));
    }
    String msgFilter = null;
    if (cl.hasOption(o.getOption("m").getOpt()))
      msgFilter = cl.getOptionValue(o.getOption("m").getOpt());
    String levelFilter = null;
    if (cl.hasOption(o.getOption("l").getOpt()))
      levelFilter = cl.getOptionValue(o.getOption("l").getOpt());
    
    SendLogToChainsaw c = new SendLogToChainsaw(logDir, fileNameFilter, chainsawHost, chainsawPort, startDate, endDate, msgFilter, levelFilter);
    c.processLogFiles();
  }
  
}
