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

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import org.apache.accumulo.core.cli.Help;
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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

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

  public void processLogFiles() throws IOException {
    String line = null;
    String out = null;
    InputStreamReader isReader = null;
    BufferedReader reader = null;
    try {
      for (File log : logFiles) {
        // Parse the server type and name from the log file name
        String threadName = log.getName().substring(0, log.getName().indexOf("."));
        try {
          isReader = new InputStreamReader(new FileInputStream(log), UTF_8);
        } catch (FileNotFoundException e) {
          System.out.println("Unable to find file: " + log.getAbsolutePath());
          throw e;
        }
        reader = new BufferedReader(isReader);

        try {
          line = reader.readLine();
          while (null != line) {
            out = convertLine(line, threadName);
            if (null != out) {
              if (socket != null && socket.isConnected())
                socket.getOutputStream().write(out.getBytes(UTF_8));
              else
                System.err.println("Unable to send data to transport");
            }
            line = reader.readLine();
          }
        } catch (IOException e) {
          System.out.println("Error processing line: " + line + ". Output was " + out);
          throw e;
        } finally {
          if (reader != null) {
            reader.close();
          }
          if (isReader != null) {
            isReader.close();
          }
        }
      }
    } finally {
      if (socket != null && socket.isConnected()) {
        socket.close();
      }
    }
  }

  private String convertLine(String line, String threadName) throws UnsupportedEncodingException {
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
      message = URLEncoder.encode(message, UTF_8.name());
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

  private static class DateConverter implements IStringConverter<Date> {
    @Override
    public Date convert(String value) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      try {
        return formatter.parse(value);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }

  }

  private static class Opts extends Help {

    @Parameter(names = {"-d", "--logDirectory"}, description = "ACCUMULO log directory path", required = true)
    String dir;

    @Parameter(names = {"-f", "--fileFilter"}, description = "filter to apply to names of logs")
    String filter;

    @Parameter(names = {"-h", "--host"}, description = "host where chainsaw is running", required = true)
    String hostname;

    @Parameter(names = {"-p", "--port"}, description = "port where XMLSocketReceiver is listening", required = true)
    int portnum;

    @Parameter(names = {"-s", "--start"}, description = "start date filter (yyyyMMddHHmmss)", required = true, converter = DateConverter.class)
    Date startDate;

    @Parameter(names = {"-e", "--end"}, description = "end date filter (yyyyMMddHHmmss)", required = true, converter = DateConverter.class)
    Date endDate;

    @Parameter(names = {"-l", "--level"}, description = "filter log level")
    String level;

    @Parameter(names = {"-m", "--messageFilter"}, description = "regex filter for log messages")
    String regex;
  }

  /**
   *
   * @param args
   *          <ol>
   *          <li>path to log directory</li>
   *          <li>filter to apply for logs to include (uses wildcards (i.e. logger* and IS case sensitive)</li>
   *          <li>chainsaw host</li>
   *          <li>chainsaw port</li>
   *          <li>start date filter</li>
   *          <li>end date filter</li>
   *          <li>optional regex filter to match on each log4j message</li>
   *          <li>optional level filter</li>
   *          </ol>
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(SendLogToChainsaw.class.getName(), args);

    SendLogToChainsaw c = new SendLogToChainsaw(opts.dir, opts.filter, opts.hostname, opts.portnum, opts.startDate, opts.endDate, opts.regex, opts.level);
    c.processLogFiles();
  }

}
