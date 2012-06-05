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
package org.apache.accumulo.core.util.shell;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import jline.ConsoleReader;
import jline.History;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.iterators.AggregatingIterator;
import org.apache.accumulo.core.iterators.FilteringIterator;
import org.apache.accumulo.core.iterators.GrepIterator;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.NoLabelIterator;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import org.apache.accumulo.core.iterators.RegExIterator;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.iterators.VersioningIterator;
import org.apache.accumulo.core.iterators.aggregation.conf.AggregatorConfiguration;
import org.apache.accumulo.core.iterators.filter.AgeOffFilter;
import org.apache.accumulo.core.iterators.filter.RegExFilter;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.TraceDump;
import org.apache.accumulo.core.trace.TraceDump.Printer;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.BulkImportHelper.AssignmentStats;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Duration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.DeleterFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterFactory;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import cloudtrace.instrument.Trace;

/**
 * A convenient console interface to perform basic accumulo functions Includes auto-complete, help, and quoted strings with escape sequences
 */
public class Shell {
  private static final Logger log = Logger.getLogger(Shell.class);
  
  private static final int NO_FIXED_ARG_LENGTH_CHECK = -1;
  private static final String SHELL_DESCRIPTION = "Shell - Apache Accumulo Interactive Shell";
  private static final String DEFAULT_AUTH_TIMEOUT = "60"; // in minutes
  
  private int exitCode = 0;
  
  private String tableName;
  private Instance instance;
  private Connector connector;
  public ConsoleReader reader;
  private AuthInfo credentials;
  private Class<? extends Formatter> formatterClass = DefaultFormatter.class;
  private Map<String,Map<String,Map<String,String>>> scanIteratorOptions = new HashMap<String,Map<String,Map<String,String>>>();
  
  private Token rootToken;
  private CommandFactory commandFactory;
  private boolean configError = false;
  
  // Global options flags
  static final String userOption = "u";
  static final String tableOption = "t";
  static final String helpOption = "?";
  static final String helpLongOption = "help";
  
  // exit if true
  private boolean exit = false;
  
  // file to execute commands from
  private String execFile = null;
  private boolean verbose = true;
  
  private boolean tabCompletion;
  private boolean disableAuthTimeout;
  private long authTimeout;
  private long lastUserActivity = System.currentTimeMillis();
  
  public void config(String... args) {
    Options opts = new Options();
    
    Option usernameOption = new Option("u", "user", true, "username (defaults to your OS user)");
    usernameOption.setArgName("user");
    opts.addOption(usernameOption);
    
    Option passwOption = new Option("p", "password", true, "password (prompt for password if this option is missing)");
    passwOption.setArgName("pass");
    opts.addOption(passwOption);
    
    Option tabCompleteOption = new Option(null, "disable-tab-completion", false, "disables tab completion (for less overhead when scripting)");
    opts.addOption(tabCompleteOption);
    
    Option debugOption = new Option(null, "debug", false, "enables client debugging");
    opts.addOption(debugOption);
    
    Option fakeOption = new Option(null, "fake", false, "fake a connection to accumulo");
    opts.addOption(fakeOption);
    
    Option helpOpt = new Option(helpOption, helpLongOption, false, "display this help");
    opts.addOption(helpOpt);
    
    OptionGroup execFileGroup = new OptionGroup();
    
    Option execfileOption = new Option("f", "execute-file", true, "executes commands from a file at startup");
    execfileOption.setArgName("file");
    execFileGroup.addOption(execfileOption);
    
    Option execfileVerboseOption = new Option("fv", "execute-file-verbose", true, "executes commands from a file at startup, with commands shown");
    execfileVerboseOption.setArgName("file");
    execFileGroup.addOption(execfileVerboseOption);
    
    opts.addOptionGroup(execFileGroup);
    
    OptionGroup instanceOptions = new OptionGroup();
    
    Option hdfsZooInstance = new Option("h", "hdfsZooInstance", false, "use hdfs zoo instance");
    instanceOptions.addOption(hdfsZooInstance);
    
    Option zooKeeperInstance = new Option("z", "zooKeeperInstance", true, "use a zookeeper instance with the given instance name and list of zoo hosts");
    zooKeeperInstance.setArgName("name hosts");
    zooKeeperInstance.setArgs(2);
    instanceOptions.addOption(zooKeeperInstance);
    
    opts.addOptionGroup(instanceOptions);
    
    OptionGroup authTimeoutOptions = new OptionGroup();
    
    Option authTimeoutOpt = new Option(null, "auth-timeout", true, "minutes the shell can be idle without re-entering a password (default "
        + DEFAULT_AUTH_TIMEOUT + " min)");
    authTimeoutOpt.setArgName("minutes");
    authTimeoutOptions.addOption(authTimeoutOpt);
    
    Option disableAuthTimeoutOpt = new Option(null, "disable-auth-timeout", false, "disables requiring the user to re-type a password after being idle");
    authTimeoutOptions.addOption(disableAuthTimeoutOpt);
    
    opts.addOptionGroup(authTimeoutOptions);
    
    CommandLine cl;
    try {
      cl = new BasicParser().parse(opts, args);
      if (cl.getArgs().length > 0)
        throw new ParseException("Unrecognized arguments: " + cl.getArgList());
      
      if (cl.hasOption(helpOpt.getOpt())) {
        configError = true;
        printHelp("shell", SHELL_DESCRIPTION, opts);
        return;
      }
      
      setDebugging(cl.hasOption(debugOption.getLongOpt()));
      authTimeout = Integer.parseInt(cl.getOptionValue(authTimeoutOpt.getLongOpt(), DEFAULT_AUTH_TIMEOUT)) * 60 * 1000;
      disableAuthTimeout = cl.hasOption(disableAuthTimeoutOpt.getLongOpt());
      
      if (cl.hasOption(zooKeeperInstance.getOpt()) && cl.getOptionValues(zooKeeperInstance.getOpt()).length != 2)
        throw new MissingArgumentException(zooKeeperInstance);
      
    } catch (Exception e) {
      configError = true;
      printException(e);
      printHelp("shell", SHELL_DESCRIPTION, opts);
      return;
    }
    
    // get the options that were parsed
    String sysUser = System.getProperty("user.name");
    if (sysUser == null)
      sysUser = "root";
    String user = cl.getOptionValue(usernameOption.getOpt(), sysUser);
    
    String passw = cl.getOptionValue(passwOption.getOpt(), null);
    tabCompletion = !cl.hasOption(tabCompleteOption.getLongOpt());
    
    // should only be one instance option set
    instance = null;
    if (cl.hasOption(fakeOption.getLongOpt())) {
      instance = new MockInstance();
    } else if (cl.hasOption(hdfsZooInstance.getOpt())) {
      instance = HdfsZooInstance.getInstance();
    } else if (cl.hasOption(zooKeeperInstance.getOpt())) {
      String[] zkOpts = cl.getOptionValues(zooKeeperInstance.getOpt());
      instance = new ZooKeeperInstance(zkOpts[0], zkOpts[1]);
    } else {
      instance = HdfsZooInstance.getInstance();
    }
    
    // process default parameters if unspecified
    byte[] pass;
    try {
      if (!cl.hasOption(fakeOption.getLongOpt())) {
        DistributedTrace.enable(instance, "shell", InetAddress.getLocalHost().getHostName());
      }
      
      this.reader = new ConsoleReader();
      
      if (passw == null)
        passw = reader.readLine("Enter current password for '" + user + "'@'" + instance.getInstanceName() + "': ", '*');
      if (passw == null) {
        reader.printNewline();
        configError = true;
        return;
      } // user canceled
      
      pass = passw.getBytes();
      this.tableName = "";
      connector = instance.getConnector(user, pass);
      this.credentials = new AuthInfo(user, pass, connector.getInstance().getInstanceID());
      
    } catch (Exception e) {
      printException(e);
      configError = true;
    }
    
    // decide whether to execute commands from a file and quit
    if (cl.hasOption(execfileOption.getOpt())) {
      execFile = cl.getOptionValue(execfileOption.getOpt());
      verbose = false;
    } else if (cl.hasOption(execfileVerboseOption.getOpt()))
      execFile = cl.getOptionValue(execfileVerboseOption.getOpt());
    
    rootToken = new Token();
    commandFactory = new CommandFactory();
  }
  
  Connector getConnector() {
    return connector;
  }
  
  public static void main(String args[]) throws IOException {
    Shell shell = new Shell();
    shell.config(args);
    
    System.exit(shell.start());
  }
  
  public int start() throws IOException {
    if (configError)
      return 1;
    
    String input;
    if (verbose)
      printInfo();
    
    String configDir = System.getenv("HOME") + "/.accumulo";
    String historyPath = configDir + "/shell_history.txt";
    File accumuloDir = new File(configDir);
    if (!accumuloDir.exists() && !accumuloDir.mkdirs())
      log.warn("Unable to make directory for history at " + accumuloDir);
    try {
      History history = new History();
      history.setHistoryFile(new File(historyPath));
      reader.setHistory(history);
    } catch (IOException e) {
      log.warn("Unable to load history file at " + historyPath);
    }
    
    commandFactory.buildCommands();
    
    ShellCompletor userCompletor = null;
    
    if (execFile != null) {
      java.util.Scanner scanner = new java.util.Scanner(new File(execFile));
      while (scanner.hasNextLine())
        execCommand(scanner.nextLine(), true, verbose);
    }
    
    while (true) {
      if (exit)
        return exitCode;
      
      // If tab completion is true we need to reset
      if (tabCompletion) {
        if (userCompletor != null)
          reader.removeCompletor(userCompletor);
        
        userCompletor = setupCompletion();
        reader.addCompletor(userCompletor);
      }
      
      reader.setDefaultPrompt(getDefaultPrompt());
      input = reader.readLine();
      if (input == null) {
        reader.printNewline();
        return exitCode;
      } // user canceled
      
      execCommand(input, disableAuthTimeout, false);
    }
  }
  
  private void printInfo() throws IOException {
    reader.printString("\n" + SHELL_DESCRIPTION + "\n" + "- \n" + "- version: " + Constants.VERSION + "\n" + "- instance name: "
        + connector.getInstance().getInstanceName() + "\n" + "- instance id: " + connector.getInstance().getInstanceID() + "\n" + "- \n"
        + "- type 'help' for a list of available commands\n" + "- \n");
  }
  
  private void printVerboseInfo() throws IOException {
    StringBuilder sb = new StringBuilder("-\n");
    sb.append("- Current user: ").append(connector.whoami()).append("\n");
    if (execFile != null)
      sb.append("- Executing commands from: ").append(execFile).append("\n");
    if (disableAuthTimeout)
      sb.append("- Authorization timeout: disabled\n");
    else
      sb.append("- Authorization timeout: ").append(String.format("%.2fs\n", authTimeout / 1000.0));
    sb.append("- Debug: ").append(isDebuggingEnabled() ? "on" : "off").append("\n");
    if (formatterClass != null && formatterClass != DefaultFormatter.class) {
      sb.append("- Active formatter class: ").append(formatterClass.getSimpleName()).append("\n");
    }
    if (!scanIteratorOptions.isEmpty()) {
      for (Entry<String,Map<String,Map<String,String>>> entry : scanIteratorOptions.entrySet()) {
        sb.append("- Session scan iterators for table ").append(entry.getKey()).append(":\n");
        for (Entry<String,Map<String,String>> namedEntry : entry.getValue().entrySet()) {
          sb.append("-    Iterator ").append(namedEntry.getKey()).append(" options:\n");
          for (Entry<String,String> optEntry : namedEntry.getValue().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-\n");
    reader.printString(sb.toString());
  }
  
  private String getDefaultPrompt() {
    return connector.whoami() + "@" + connector.getInstance().getInstanceName() + (tableName.isEmpty() ? "" : " ") + tableName + "> ";
  }
  
  private void execCommand(String input, boolean ignoreAuthTimeout, boolean echoPrompt) throws IOException {
    if (echoPrompt) {
      reader.printString(getDefaultPrompt());
      reader.printString(input);
      reader.printNewline();
    }
    
    String fields[];
    try {
      fields = new QuotedStringTokenizer(input).getTokens();
    } catch (BadArgumentException e) {
      printException(e);
      ++exitCode;
      return;
    }
    if (fields.length == 0)
      return;
    
    String command = fields[0];
    fields = fields.length > 1 ? Arrays.copyOfRange(fields, 1, fields.length) : new String[] {};
    
    Command sc = null;
    if (command.length() > 0) {
      try {
        // Obtain the command from the command table
        sc = commandFactory.getCommandByName(command);
        
        if (!(sc instanceof ExitCommand) && !ignoreAuthTimeout && System.currentTimeMillis() - lastUserActivity > authTimeout) {
          reader.printString("Shell has been idle for too long. Please re-authenticate.\n");
          boolean authFailed = true;
          do {
            String pwd = reader.readLine("Enter current password for '" + connector.whoami() + "': ", '*');
            if (pwd == null) {
              reader.printNewline();
              return;
            } // user canceled
            
            try {
              authFailed = !connector.securityOperations().authenticateUser(connector.whoami(), pwd.getBytes());
            } catch (Exception e) {
              ++exitCode;
              printException(e);
            }
            
            if (authFailed)
              reader.printString("Invalid password. ");
          } while (authFailed);
          lastUserActivity = System.currentTimeMillis();
        }
        
        // Get the options from the command on how to parse the string
        Options parseOpts = sc.getOptionsWithHelp();
        
        // Parse the string using the given options
        CommandLine cl = new BasicParser().parse(parseOpts, fields);
        
        int actualArgLen = cl.getArgs().length;
        int expectedArgLen = sc.numArgs();
        if (cl.hasOption(helpOption)) {
          // Display help if asked to; otherwise execute the command
          sc.printHelp();
        } else if (expectedArgLen != NO_FIXED_ARG_LENGTH_CHECK && actualArgLen != expectedArgLen) {
          ++exitCode;
          // Check for valid number of fixed arguments (if not
          // negative; negative means it is not checked, for
          // vararg-like commands)
          printException(new IllegalArgumentException(String.format("Expected %d argument%s. There %s %d.", expectedArgLen, expectedArgLen == 1 ? "" : "s",
              actualArgLen == 1 ? "was" : "were", actualArgLen)));
          sc.printHelp();
        } else {
          int tmpCode = sc.execute(input, cl, this);
          exitCode += tmpCode;
          reader.flushConsole();
        }
        
      } catch (ConstraintViolationException e) {
        ++exitCode;
        printConstraintViolationException(e);
      } catch (TableNotFoundException e) {
        ++exitCode;
        if (tableName.equals(e.getTableName()))
          tableName = "";
        printException(e);
      } catch (ParseException e) {
        // not really an error if the exception is a missing required
        // option when the user is asking for help
        if (!(e instanceof MissingOptionException && (Arrays.asList(fields).contains("-" + helpOption) || Arrays.asList(fields).contains("--" + helpLongOption)))) {
          ++exitCode;
          printException(e);
        }
        if (sc != null)
          sc.printHelp();
      } catch (Exception e) {
        ++exitCode;
        printException(e);
      }
    } else {
      ++exitCode;
      printException(new BadArgumentException("Unrecognized empty command", command, -1));
    }
  }
  
  /**
   * The command tree is built in reverse so that the references are more easily linked up. There is some code in token to allow forward building of the command
   * tree.
   */
  private ShellCompletor setupCompletion() {
    rootToken = new Token();
    
    Set<String> tableNames = null;
    try {
      tableNames = connector.tableOperations().list();
    } catch (Exception e) {
      log.debug("Unable to obtain list of tables", e);
      tableNames = Collections.emptySet();
    }
    
    Set<String> userlist = null;
    try {
      userlist = connector.securityOperations().listUsers();
    } catch (Exception e) {
      log.debug("Unable to obtain list of users", e);
      userlist = Collections.emptySet();
    }
    
    Map<Command.CompletionSet,Set<String>> options = new HashMap<Command.CompletionSet,Set<String>>();
    
    Set<String> commands = new HashSet<String>();
    for (String a : commandFactory.getCommandTable().keySet())
      commands.add(a);
    
    Set<String> modifiedUserlist = new HashSet<String>();
    Set<String> modifiedTablenames = new HashSet<String>();
    
    for (String a : tableNames)
      modifiedTablenames.add(a.replaceAll("([\\s'\"])", "\\\\$1"));
    for (String a : userlist)
      modifiedUserlist.add(a.replaceAll("([\\s'\"])", "\\\\$1"));
    
    options.put(Command.CompletionSet.USERNAMES, modifiedUserlist);
    options.put(Command.CompletionSet.TABLENAMES, modifiedTablenames);
    options.put(Command.CompletionSet.COMMANDS, commands);
    
    for (Entry<String,CommandFactory.Attributes> a : commandFactory.getCommandTable().entrySet()) {
      switch (a.getValue().codetype) {
        case JAVA:
          try {
            Command c = commandFactory.getCommandByName(a.getKey());
            c.getOptionsWithHelp(); // prep the options for the command
            // so that the completion can
            // include them
            c.registerCompletion(rootToken, options);
          } catch (ShellCommandException e) {
            ++exitCode;
            printException(e);
          }
        default:
          // no other code supported
      }
    }
    return new ShellCompletor(rootToken, options);
  }
  
  /**
   * Command Factory is a class that holds the command table, which is a mapping from a string representing the command to a class. It has methods to build the
   * command table from the classes in this file and a currently unused method to build the command table from XML. The classes that the table returns all
   * extend Shell$Command class.
   */
  public static class CommandFactory {
    private TreeMap<String,Attributes> commandTable = new TreeMap<String,Attributes>();
    
    public enum CodeType {
      JAVA, PYTHON, RUBY, C;
    }
    
    private static class Attributes {
      public String location;
      public CodeType codetype;
    }
    
    public Map<String,Attributes> getCommandTable() {
      return commandTable;
    }
    
    public void buildCommandsFromXML(File file) throws ShellCommandException {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder docbuilder;
      Document d = null;
      
      try {
        docbuilder = dbf.newDocumentBuilder();
        d = docbuilder.parse(file);
      } catch (Exception e) {
        throw new ShellCommandException(ShellCommandException.ErrorCode.XML_PARSING_ERROR);
      }
      
      Attributes a;
      
      NodeList nodelist = d.getDocumentElement().getElementsByTagName("command");
      NodeList sub_nl;
      for (int i = 0; i < nodelist.getLength(); i++) {
        a = new Attributes();
        Element el = (Element) nodelist.item(i);
        
        sub_nl = el.getElementsByTagName("name");
        Element name = (Element) sub_nl.item(0);
        
        sub_nl = el.getElementsByTagName("location");
        Element loc = (Element) sub_nl.item(0);
        
        sub_nl = el.getElementsByTagName("codetype");
        Element ct = (Element) sub_nl.item(0);
        
        a.location = loc.getTextContent();
        a.codetype = CodeType.valueOf(ct.getTextContent());
        
        commandTable.put(name.getTextContent(), a);
      }
    }
    
    public Command getCommandByName(String command) throws ShellCommandException {
      command = command.toLowerCase(Locale.ENGLISH);
      Attributes toexec = commandTable.get(command);
      
      if (toexec != null) {
        Command sc = null;
        switch (toexec.codetype) {
          case JAVA:
            try {
              sc = (Command) Class.forName(toexec.location).newInstance();
            } catch (Exception e) {
              throw new ShellCommandException(ShellCommandException.ErrorCode.INITIALIZATION_FAILURE, command);
            }
            break;
          case PYTHON:
          case RUBY:
          case C:
          default:
            throw new ShellCommandException(ShellCommandException.ErrorCode.UNSUPPORTED_LANGUAGE, command);
        }
        return sc;
      }
      throw new ShellCommandException(ShellCommandException.ErrorCode.UNRECOGNIZED_COMMAND, command);
    }
    
    public void buildCommands() {
      for (Class<?> subclass : Shell.class.getClasses()) {
        Object t;
        try {
          t = subclass.newInstance();
        } catch (Exception e) {
          continue;
        }
        Attributes attr;
        if (t instanceof Command) {
          Command sci = (Command) t;
          attr = new Attributes();
          attr.location = sci.getClass().getName();
          if (attr.location == null || attr.location.isEmpty())
            continue;
          attr.codetype = CodeType.JAVA;
          
          // Don't want to add duplicate entries
          if (!commandTable.containsKey(sci.getName()))
            commandTable.put(sci.getName(), attr);
        }
      }
    }
    
  }
  
  /**
   * The Command class represents a command to be run in the shell. It contains the methods to execute along with some methods to help tab completion, and
   * return the command name, help, and usage.
   */
  public static abstract class Command {
    // Helper methods for completion
    public enum CompletionSet {
      TABLENAMES, USERNAMES, COMMANDS
    }
    
    static Set<String> getCommandNames(Map<CompletionSet,Set<String>> objects) {
      return objects.get(CompletionSet.COMMANDS);
    }
    
    static Set<String> getTableNames(Map<CompletionSet,Set<String>> objects) {
      return objects.get(CompletionSet.TABLENAMES);
    }
    
    static Set<String> getUserNames(Map<CompletionSet,Set<String>> objects) {
      return objects.get(CompletionSet.USERNAMES);
    }
    
    public void registerCompletionGeneral(Token root, Set<String> args, boolean caseSens) {
      Token t = new Token(args);
      t.setCaseSensitive(caseSens);
      
      Token command = new Token(getName());
      command.addSubcommand(t);
      
      root.addSubcommand(command);
    }
    
    public void registerCompletionForTables(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.TABLENAMES), true);
    }
    
    public void registerCompletionForUsers(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.USERNAMES), true);
    }
    
    public void registerCompletionForCommands(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.COMMANDS), false);
    }
    
    // abstract methods to override
    public abstract int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception;
    
    public abstract String description();
    
    public abstract int numArgs();
    
    // OPTIONAL methods to override:
    
    // the general version of getname uses reflection to get the class name
    // and then cuts off the suffix -Command to get the name of the command
    public String getName() {
      String s = this.getClass().getName();
      int st = Math.max(s.lastIndexOf('$'), s.lastIndexOf('.'));
      int i = s.indexOf("Command");
      return i > 0 ? s.substring(st + 1, i).toLowerCase(Locale.ENGLISH) : null;
    }
    
    // The general version of this method adds the name
    // of the command to the completion tree
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completion_set) {
      root.addSubcommand(new Token(getName()));
    }
    
    // The general version of this method uses the HelpFormatter
    // that comes with the apache Options package to print out the help
    public final void printHelp() {
      Shell.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp());
    }
    
    // Get options with help
    public final Options getOptionsWithHelp() {
      Options opts = getOptions();
      opts.addOption(new Option(helpOption, helpLongOption, false, "display this help"));
      return opts;
    }
    
    // General usage is just the command
    public String usage() {
      return getName();
    }
    
    // General Options are empty
    public Options getOptions() {
      return new Options();
    }
  }
  
  public static class UsersCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      for (String user : shellState.connector.securityOperations().listUsers())
        shellState.reader.printString(user + "\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "displays a list of existing users";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class MasterStateCommand extends Command {
    
    @Override
    public String description() {
      return "set the master state: NORMAL, SAFE_MODE or CLEAN_STOP";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      String state = cl.getArgs()[0];
      state = state.toUpperCase();
      if (MasterGoalState.valueOf(state) == null) {
        throw new IllegalArgumentException(state + " is not a valid master state");
      }
      ZooUtil.putPersistentData(ZooUtil.getRoot(shellState.connector.getInstance()) + Constants.ZMASTER_GOAL_STATE, state.getBytes(),
          NodeExistsPolicy.OVERWRITE);
      return 0;
    }
    
    @Override
    public String usage() {
      return getName() + " <NORMAL|SAFE_MODE|CLEAN_STOP>";
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
    
  }
  
  public static class TablePermissionsCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      for (String p : TablePermission.printableValues())
        shellState.reader.printString(p + "\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "displays a list of valid table permissions";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class ClasspathCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) {
      AccumuloClassLoader.printClassPath();
      return 0;
    }
    
    @Override
    public String description() {
      return "lists the current files on the classpath";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class SystemPermissionsCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      for (String p : SystemPermission.printableValues())
        shellState.reader.printString(p + "\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "displays a list of valid system permissions";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class AuthenticateCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      String user = cl.getArgs()[0];
      String p = shellState.reader.readLine("Enter current password for '" + user + "': ", '*');
      if (p == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      byte[] password = p.getBytes();
      boolean valid = shellState.connector.securityOperations().authenticateUser(user, password);
      shellState.reader.printString((valid ? "V" : "Not v") + "alid\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "verifies a user's credentials";
    }
    
    @Override
    public String usage() {
      return getName() + " <username>";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionForUsers(root, completionSet);
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class CreateUserCommand extends Command {
    private Option scanOptAuths;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, TableNotFoundException, AccumuloSecurityException,
        TableExistsException, IOException {
      String user = cl.getArgs()[0];
      String password = null;
      String passwordConfirm = null;
      
      password = shellState.reader.readLine("Enter new password for '" + user + "': ", '*');
      if (password == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      passwordConfirm = shellState.reader.readLine("Please confirm new password for '" + user + "': ", '*');
      if (passwordConfirm == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      
      if (!password.equals(passwordConfirm))
        throw new IllegalArgumentException("Passwords do not match");
      
      Authorizations authorizations = parseAuthorizations(cl.hasOption(scanOptAuths.getOpt()) ? cl.getOptionValue(scanOptAuths.getOpt()) : "");
      shellState.connector.securityOperations().createUser(user, password.getBytes(), authorizations);
      log.debug("Created user " + user + " with" + (authorizations.isEmpty() ? " no" : "") + " initial scan authorizations"
          + (!authorizations.isEmpty() ? " " + authorizations : ""));
      return 0;
    }
    
    @Override
    public String usage() {
      return getName() + " <username>";
    }
    
    @Override
    public String description() {
      return "creates a new user";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations");
      scanOptAuths.setArgName("comma-separated-authorizations");
      o.addOption(scanOptAuths);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class DeleteUserCommand extends DropUserCommand {}
  
  public static class DropUserCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
      String user = cl.getArgs()[0];
      if (shellState.connector.whoami().equals(user))
        throw new BadArgumentException("You cannot delete yourself", fullCommand, fullCommand.indexOf(user));
      shellState.connector.securityOperations().dropUser(user);
      log.debug("Deleted user " + user);
      return 0;
    }
    
    @Override
    public String description() {
      return "deletes a user";
    }
    
    @Override
    public String usage() {
      return getName() + " <username>";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionForUsers(root, completionSet);
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class PasswdCommand extends Command {
    private Option userOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      String currentUser = shellState.connector.whoami();
      String user = cl.getOptionValue(userOpt.getOpt(), currentUser);
      
      String password = null;
      String passwordConfirm = null;
      String oldPassword = null;
      
      oldPassword = shellState.reader.readLine("Enter current password for '" + currentUser + "': ", '*');
      if (oldPassword == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      
      if (!shellState.connector.securityOperations().authenticateUser(currentUser, oldPassword.getBytes()))
        throw new AccumuloSecurityException(user, SecurityErrorCode.BAD_CREDENTIALS);
      
      password = shellState.reader.readLine("Enter new password for '" + user + "': ", '*');
      if (password == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      passwordConfirm = shellState.reader.readLine("Please confirm new password for '" + user + "': ", '*');
      if (passwordConfirm == null) {
        shellState.reader.printNewline();
        return 0;
      } // user canceled
      
      if (!password.equals(passwordConfirm))
        throw new IllegalArgumentException("Passwords do not match");
      
      byte[] pass = password.getBytes();
      shellState.connector.securityOperations().changeUserPassword(user, pass);
      // update the current credentials if the password changed was for
      // the current user
      if (shellState.connector.whoami().equals(user))
        shellState.credentials = new AuthInfo(user, pass, shellState.connector.getInstance().getInstanceID());
      log.debug("Changed password for user " + user);
      return 0;
    }
    
    @Override
    public String description() {
      return "changes a user's password";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
      userOpt.setArgName("user");
      o.addOption(userOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class SetAuthsCommand extends Command {
    private Option userOpt;
    private Option scanOptAuths;
    private Option clearOptAuths;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
      String user = cl.getOptionValue(userOpt.getOpt(), shellState.connector.whoami());
      String scanOpts = cl.hasOption(clearOptAuths.getOpt()) ? null : cl.getOptionValue(scanOptAuths.getOpt());
      shellState.connector.securityOperations().changeUserAuthorizations(user, parseAuthorizations(scanOpts));
      log.debug("Changed record-level authorizations for user " + user);
      return 0;
    }
    
    @Override
    public String description() {
      return "sets the maximum scan authorizations for a user";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionForUsers(root, completionSet);
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      OptionGroup setOrClear = new OptionGroup();
      scanOptAuths = new Option("s", "scan-authorizations", true, "set the scan authorizations");
      scanOptAuths.setArgName("comma-separated-authorizations");
      setOrClear.addOption(scanOptAuths);
      clearOptAuths = new Option("c", "clear-authorizations", false, "clears the scan authorizations");
      setOrClear.addOption(clearOptAuths);
      setOrClear.setRequired(true);
      o.addOptionGroup(setOrClear);
      userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
      userOpt.setArgName("user");
      o.addOption(userOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class GetAuthsCommand extends Command {
    private Option userOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      String user = cl.getOptionValue(userOpt.getOpt(), shellState.connector.whoami());
      shellState.reader.printString(shellState.connector.securityOperations().getUserAuthorizations(user) + "\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "displays the maximum scan authorizations for a user";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
      userOpt.setArgName("user");
      o.addOption(userOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class UserPermissionsCommand extends Command {
    private Option userOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      String user = cl.getOptionValue(userOpt.getOpt(), shellState.connector.whoami());
      
      String delim = "";
      shellState.reader.printString("System permissions: ");
      for (SystemPermission p : SystemPermission.values()) {
        if (shellState.connector.securityOperations().hasSystemPermission(user, p)) {
          shellState.reader.printString(delim + "System." + p.name());
          delim = ", ";
        }
      }
      
      shellState.reader.printString(delim.equals("") ? "NONE" : "");
      shellState.reader.printNewline();
      
      for (String t : shellState.connector.tableOperations().list()) {
        delim = "";
        shellState.reader.printString("Table permissions (" + t + "): ");
        for (TablePermission p : TablePermission.values()) {
          if (shellState.connector.securityOperations().hasTablePermission(user, t, p)) {
            shellState.reader.printString(delim + "Table." + p.name());
            delim = ", ";
          }
        }
        shellState.reader.printString(delim.equals("") ? "NONE" : "");
        shellState.reader.printNewline();
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "displays a user's system and table permissions";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
      userOpt.setArgName("user");
      o.addOption(userOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class GrantCommand extends Command {
    private Option systemOpt;
    private Option tableOpt;
    private Option userOpt;
    private Option tablePatternOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
      String user = cl.hasOption(userOpt.getOpt()) ? cl.getOptionValue(userOpt.getOpt()) : shellState.connector.whoami();
      
      String permission[] = cl.getArgs()[0].split("\\.", 2);
      if (cl.hasOption(systemOpt.getOpt()) && permission[0].equalsIgnoreCase("System")) {
        try {
          shellState.connector.securityOperations().grantSystemPermission(user, SystemPermission.valueOf(permission[1]));
          log.debug("Granted " + user + " the " + permission[1] + " permission");
        } catch (IllegalArgumentException e) {
          throw new BadArgumentException("No such system permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        }
      } else if (permission[0].equalsIgnoreCase("Table")) {
        if (cl.hasOption(tableOpt.getOpt())) {
          String tableName = cl.getOptionValue(tableOpt.getOpt());
          try {
            shellState.connector.securityOperations().grantTablePermission(user, tableName, TablePermission.valueOf(permission[1]));
            log.debug("Granted " + user + " the " + permission[1] + " permission on table " + tableName);
          } catch (IllegalArgumentException e) {
            throw new BadArgumentException("No such table permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
          }
        } else if (cl.hasOption(tablePatternOpt.getOpt())) {
          for (String tableName : shellState.connector.tableOperations().list()) {
            if (tableName.matches(cl.getOptionValue(tablePatternOpt.getOpt()))) {
              try {
                shellState.connector.securityOperations().grantTablePermission(user, tableName, TablePermission.valueOf(permission[1]));
                log.debug("Granted " + user + " the " + permission[1] + " permission on table " + tableName);
              } catch (IllegalArgumentException e) {
                throw new BadArgumentException("No such table permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
              }
            }
          }
        } else {
          throw new BadArgumentException("You must provide a table name", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        }
      } else {
        throw new BadArgumentException("Unrecognized permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "grants system or table permissions for a user";
    }
    
    @Override
    public String usage() {
      return getName() + " <permission>";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      Token cmd = new Token(getName());
      cmd.addSubcommand(new Token(TablePermission.printableValues()));
      cmd.addSubcommand(new Token(SystemPermission.printableValues()));
      root.addSubcommand(cmd);
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      OptionGroup group = new OptionGroup();
      
      tableOpt = new Option(Shell.tableOption, "table", true, "grant a table permission on this table");
      systemOpt = new Option("s", "system", false, "grant a system permission");
      tablePatternOpt = new Option("p", "pattern", true, "regex pattern of tables to grant permissions on");
      tablePatternOpt.setArgName("pattern");
      
      tableOpt.setArgName("table");
      
      group.addOption(systemOpt);
      group.addOption(tableOpt);
      group.addOption(tablePatternOpt);
      group.setRequired(true);
      
      o.addOptionGroup(group);
      userOpt = new Option(Shell.userOption, "user", true, "user to operate on");
      userOpt.setArgName("username");
      userOpt.setRequired(true);
      o.addOption(userOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class RevokeCommand extends Command {
    private Option systemOpt;
    private Option tableOpt;
    private Option userOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException {
      String user = cl.hasOption(userOpt.getOpt()) ? cl.getOptionValue(userOpt.getOpt()) : shellState.connector.whoami();
      
      String permission[] = cl.getArgs()[0].split("\\.", 2);
      if (cl.hasOption(systemOpt.getOpt()) && permission[0].equalsIgnoreCase("System")) {
        try {
          shellState.connector.securityOperations().revokeSystemPermission(user, SystemPermission.valueOf(permission[1]));
          log.debug("Revoked from " + user + " the " + permission[1] + " permission");
        } catch (IllegalArgumentException e) {
          throw new BadArgumentException("No such system permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        }
      } else if (cl.hasOption(tableOpt.getOpt()) && permission[0].equalsIgnoreCase("Table")) {
        String tableName = cl.getOptionValue(tableOpt.getOpt());
        try {
          shellState.connector.securityOperations().revokeTablePermission(user, tableName, TablePermission.valueOf(permission[1]));
          log.debug("Revoked from " + user + " the " + permission[1] + " permission on table " + tableName);
        } catch (IllegalArgumentException e) {
          throw new BadArgumentException("No such table permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
        }
      } else {
        throw new BadArgumentException("Unrecognized permission", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "revokes system or table permissions from a user";
    }
    
    @Override
    public String usage() {
      return getName() + " <permission>";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      Token cmd = new Token(getName());
      cmd.addSubcommand(new Token(TablePermission.printableValues()));
      cmd.addSubcommand(new Token(SystemPermission.printableValues()));
      root.addSubcommand(cmd);
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      OptionGroup group = new OptionGroup();
      
      tableOpt = new Option(tableOption, "table", true, "revoke a table permission on this table");
      systemOpt = new Option("s", "system", false, "revoke a system permission");
      
      tableOpt.setArgName("table");
      
      group.addOption(systemOpt);
      group.addOption(tableOpt);
      group.setRequired(true);
      
      o.addOptionGroup(group);
      
      userOpt = new Option(userOption, "user", true, "user to operate on");
      userOpt.setArgName("username");
      userOpt.setRequired(true);
      o.addOption(userOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class QuitCommand extends ExitCommand {}
  
  public static class ByeCommand extends ExitCommand {}
  
  public static class ExitCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) {
      shellState.exit = true;
      return 0;
    }
    
    @Override
    public String description() {
      return "exits the shell";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class QuestionCommand extends HelpCommand {
    @Override
    public String getName() {
      return "?";
    }
  }
  
  public static class HelpCommand extends Command {
    private Option disablePaginationOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws ShellCommandException, IOException {
      // print help summary
      if (cl.getArgs().length == 0) {
        int i = 0;
        for (String cmd : shellState.commandFactory.getCommandTable().keySet())
          i = Math.max(i, cmd.length());
        ArrayList<String> output = new ArrayList<String>();
        for (String cmd : shellState.commandFactory.getCommandTable().keySet()) {
          Command c = shellState.commandFactory.getCommandByName(cmd);
          if (!(c instanceof HiddenCommand))
            output.add(String.format("%-" + i + "s  -  %s", c.getName(), c.description()));
        }
        shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
      }
      
      // print help for every command on command line
      for (String cmd : cl.getArgs()) {
        try {
          shellState.commandFactory.getCommandByName(cmd).printHelp();
        } catch (ShellCommandException e) {
          printException(e);
          return 1;
        }
      }
      return 0;
    }
    
    public String description() {
      return "provides information about the available commands";
    }
    
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> special) {
      registerCompletionForCommands(root, special);
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      o.addOption(disablePaginationOpt);
      return o;
    }
    
    @Override
    public String usage() {
      return getName() + " [ <command>{ <command>} ]";
    }
    
    @Override
    public int numArgs() {
      return Shell.NO_FIXED_ARG_LENGTH_CHECK;
    }
  }
  
  public static class DebugCommand extends Command {
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      if (cl.getArgs().length == 1) {
        if (cl.getArgs()[0].equalsIgnoreCase("on"))
          Shell.setDebugging(true);
        else if (cl.getArgs()[0].equalsIgnoreCase("off"))
          Shell.setDebugging(false);
        else
          throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      } else if (cl.getArgs().length == 0) {
        shellState.reader.printString(Shell.isDebuggingEnabled() ? "on\n" : "off\n");
      } else {
        printException(new IllegalArgumentException("Expected 0 or 1 argument. There were " + cl.getArgs().length + "."));
        printHelp();
        return 1;
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "turns debug logging on or off";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> special) {
      Token debug_command = new Token(getName());
      debug_command.addSubcommand(Arrays.asList(new String[] {"on", "off"}));
      root.addSubcommand(debug_command);
    }
    
    @Override
    public String usage() {
      return getName() + " [ on | off ]";
    }
    
    @Override
    public int numArgs() {
      return Shell.NO_FIXED_ARG_LENGTH_CHECK;
    }
  }
  
  public static class TraceCommand extends DebugCommand {
    
    public int execute(String fullCommand, CommandLine cl, final Shell shellState) throws IOException {
      if (cl.getArgs().length == 1) {
        if (cl.getArgs()[0].equalsIgnoreCase("on")) {
          Trace.on("shell:" + shellState.credentials.user);
        } else if (cl.getArgs()[0].equalsIgnoreCase("off")) {
          if (Trace.isTracing()) {
            long trace = Trace.currentTrace().traceId();
            Trace.off();
            for (int i = 0; i < 10; i++) {
              try {
                String table = AccumuloConfiguration.getSystemConfiguration().get(Property.TRACE_TABLE);
                String user = shellState.connector.whoami();
                Authorizations auths = shellState.connector.securityOperations().getUserAuthorizations(user);
                Scanner scanner = shellState.connector.createScanner(table, auths);
                scanner.setRange(new Range(new Text(Long.toHexString(trace))));
                final StringBuffer sb = new StringBuffer();
                if (TraceDump.printTrace(scanner, new Printer() {
                  @Override
                  public void print(String line) {
                    try {
                      sb.append(line + "\n");
                    } catch (Exception ex) {
                      throw new RuntimeException(ex);
                    }
                  }
                }) > 0) {
                  shellState.reader.printString(sb.toString());
                  break;
                }
              } catch (Exception ex) {
                printException(ex);
              }
              shellState.reader.printString("Waiting for trace information\n");
              shellState.reader.flushConsole();
              UtilWaitThread.sleep(500);
            }
          } else {
            shellState.reader.printString("Not tracing\n");
          }
        } else
          throw new BadArgumentException("Argument must be 'on' or 'off'", fullCommand, fullCommand.indexOf(cl.getArgs()[0]));
      } else if (cl.getArgs().length == 0) {
        shellState.reader.printString(Trace.isTracing() ? "on\n" : "off\n");
      } else {
        printException(new IllegalArgumentException("Expected 0 or 1 argument. There were " + cl.getArgs().length + "."));
        printHelp();
        return 1;
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "turns trace logging on or off";
    }
  }
  
  public static class DeleteIterCommand extends Command {
    private Option tableOpt;
    private Option mincScopeOpt;
    private Option majcScopeOpt;
    private Option scanScopeOpt;
    private Option nameOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      if (!cl.hasOption(tableOpt.getOpt()))
        shellState.checkTableState();
      
      String tableName = cl.getOptionValue(tableOpt.getOpt(), shellState.tableName);
      if (tableName != null && !shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      String name = cl.getOptionValue(nameOpt.getOpt());
      
      ArrayList<IteratorScope> scopes = new ArrayList<IteratorScope>(3);
      if (cl.hasOption(mincScopeOpt.getOpt()))
        scopes.add(IteratorScope.minc);
      if (cl.hasOption(majcScopeOpt.getOpt()))
        scopes.add(IteratorScope.majc);
      if (cl.hasOption(scanScopeOpt.getOpt()))
        scopes.add(IteratorScope.scan);
      if (scopes.isEmpty())
        throw new IllegalArgumentException("You must select at least one scope to delete");
      
      boolean noneDeleted = true;
      String tableId = shellState.connector.tableOperations().tableIdMap().get(tableName);
      AccumuloConfiguration config = AccumuloConfiguration.getTableConfiguration(shellState.connector.getInstance().getInstanceID(), tableId);
      for (Entry<String,String> entry : config) {
        log.debug("Considering candidate for deletion " + entry.getKey());
        for (IteratorScope scope : scopes) {
          String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), name);
          log.debug("Deleting " + stem + "*");
          if (entry.getKey().startsWith(stem)) {
            noneDeleted = false;
            log.debug("removing property " + entry.getKey());
            shellState.connector.tableOperations().removeProperty(tableName, entry.getKey());
          }
        }
      }
      if (noneDeleted)
        log.warn("no iterators found that match your criteria");
      return 0;
    }
    
    @Override
    public String description() {
      return "deletes a table-specific iterator";
    }
    
    public Options getOptions() {
      Options o = new Options();
      
      tableOpt = new Option(tableOption, "table", true, "tableName");
      tableOpt.setArgName("table");
      
      nameOpt = new Option("n", "name", true, "iterator to delete");
      nameOpt.setArgName("itername");
      nameOpt.setRequired(true);
      
      mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false, "applied at minor compaction");
      majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false, "applied at major compaction");
      scanScopeOpt = new Option(IteratorScope.scan.name(), "scan-time", false, "applied at scan time");
      
      o.addOption(tableOpt);
      o.addOption(nameOpt);
      
      o.addOption(mincScopeOpt);
      o.addOption(majcScopeOpt);
      o.addOption(scanScopeOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class SetIterCommand extends Command {
    private Option tableOpt;
    private Option mincScopeOpt;
    private Option majcScopeOpt;
    private Option scanScopeOpt;
    private Option nameOpt;
    private Option priorityOpt;
    
    private Option aggTypeOpt;
    private Option filterTypeOpt;
    private Option regexTypeOpt;
    private Option versionTypeOpt;
    private Option nolabelTypeOpt;
    private Option classnameTypeOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      if (!cl.hasOption(tableOpt.getOpt()))
        shellState.checkTableState();
      
      String tableName = cl.getOptionValue(tableOpt.getOpt(), shellState.tableName);
      if (tableName != null && !shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      int priority = Integer.parseInt(cl.getOptionValue(priorityOpt.getOpt()));
      
      Map<String,String> options = new HashMap<String,String>();
      boolean filter = false;
      String classname = cl.getOptionValue(classnameTypeOpt.getOpt());
      if (cl.hasOption(aggTypeOpt.getOpt()))
        classname = AggregatingIterator.class.getName();
      else if (cl.hasOption(regexTypeOpt.getOpt()))
        classname = RegExIterator.class.getName();
      else if (cl.hasOption(versionTypeOpt.getOpt()))
        classname = VersioningIterator.class.getName();
      else if (cl.hasOption(nolabelTypeOpt.getOpt()))
        classname = NoLabelIterator.class.getName();
      else if (cl.hasOption(filterTypeOpt.getOpt()) || classname.equals(FilteringIterator.class.getName())) {
        filter = true;
        classname = FilteringIterator.class.getName();
      }
      
      String name;
      name = cl.getOptionValue(nameOpt.getOpt(), setUpOptions(shellState.reader, classname, options));
      
      if (filter) {
        Map<String,String> updates = new HashMap<String,String>();
        for (String key : options.keySet()) {
          String c = options.get(key);
          if (c.equals("ageoff")) {
            c = AgeOffFilter.class.getName();
            options.put(key, c);
          } else if (c.equals("regex")) {
            c = RegExFilter.class.getName();
            options.put(key, c);
          }
          Map<String,String> filterOptions = new HashMap<String,String>();
          setUpOptions(shellState.reader, c, filterOptions);
          
          for (Entry<String,String> e : filterOptions.entrySet())
            updates.put(key + "." + e.getKey(), e.getValue());
        }
        options.putAll(updates);
      }
      
      setTableProperties(cl, shellState, tableName, priority, options, classname, name);
      return 0;
    }
    
    protected void setTableProperties(CommandLine cl, Shell shellState, String tableName, int priority, Map<String,String> options, String classname,
        String name) throws AccumuloException, AccumuloSecurityException {
      ArrayList<IteratorScope> scopes = new ArrayList<IteratorScope>(3);
      if (cl.hasOption(mincScopeOpt.getOpt()))
        scopes.add(IteratorScope.minc);
      if (cl.hasOption(majcScopeOpt.getOpt()))
        scopes.add(IteratorScope.majc);
      if (cl.hasOption(scanScopeOpt.getOpt()))
        scopes.add(IteratorScope.scan);
      if (scopes.isEmpty())
        throw new IllegalArgumentException("You must select at least one scope to configure");
      
      for (IteratorScope scope : scopes) {
        String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), name);
        log.debug("setting property " + stem + " to " + priority + "," + classname);
        String optStem = stem + ".opt.";
        for (Entry<String,String> e : options.entrySet()) {
          log.debug("setting property " + optStem + e.getKey() + " to " + e.getValue());
          shellState.connector.tableOperations().setProperty(tableName, optStem + e.getKey(), e.getValue());
        }
        shellState.connector.tableOperations().setProperty(tableName, stem, priority + "," + classname);
      }
    }
    
    private static String setUpOptions(ConsoleReader reader, String className, Map<String,String> options) throws IOException {
      String input;
      OptionDescriber skvi;
      Class<? extends OptionDescriber> clazz;
      try {
        clazz = AccumuloClassLoader.loadClass(className, OptionDescriber.class);
        skvi = clazz.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(e.getMessage());
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(e.getMessage());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
      
      IteratorOptions itopts = skvi.describeOptions();
      if (itopts.name == null)
        throw new IllegalArgumentException(className + " described its default distinguishing name as null");
      
      Map<String,String> localOptions = new HashMap<String,String>();
      do {
        // clean up the overall options that caused things to fail
        for (String key : localOptions.keySet())
          options.remove(key);
        localOptions.clear();
        
        reader.printString(itopts.description);
        reader.printNewline();
        
        String prompt;
        if (itopts.namedOptions != null) {
          for (Entry<String,String> e : itopts.namedOptions.entrySet()) {
            prompt = repeat("-", 10) + "> set " + className + " parameter " + e.getKey() + ", " + e.getValue() + ": ";
            
            input = reader.readLine(prompt);
            if (input == null) {
              reader.printNewline();
              throw new IOException("Input stream closed");
            }
            
            if (input.length() > 0)
              localOptions.put(e.getKey(), input);
          }
        }
        
        if (itopts.unnamedOptionDescriptions != null) {
          for (String desc : itopts.unnamedOptionDescriptions) {
            reader.printString(repeat("-", 10) + "> entering options: " + desc + "\n");
            input = "start";
            while (true) {
              prompt = repeat("-", 10) + "> set " + className + " option (<name> <value>, hit enter to skip): ";
              
              input = reader.readLine(prompt);
              if (input == null) {
                reader.printNewline();
                throw new IOException("Input stream closed");
              }
              
              if (input.length() == 0)
                break;
              
              String[] sa = input.split(" ", 2);
              localOptions.put(sa[0], sa[1]);
            }
          }
        }
        
        options.putAll(localOptions);
        if (!skvi.validateOptions(options))
          reader.printString("invalid options for " + clazz.getName() + "\n");
        
      } while (!skvi.validateOptions(options));
      return itopts.name;
    }
    
    @Override
    public String description() {
      return "sets a table-specific iterator";
    }
    
    public Options getOptions() {
      Options o = new Options();
      
      tableOpt = new Option(tableOption, "table", true, "tableName");
      tableOpt.setArgName("table");
      
      priorityOpt = new Option("p", "priority", true, "the order in which the iterator is applied");
      priorityOpt.setArgName("pri");
      priorityOpt.setRequired(true);
      
      nameOpt = new Option("n", "name", true, "iterator to set");
      nameOpt.setArgName("itername");
      
      mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false, "applied at minor compaction");
      majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false, "applied at major compaction");
      scanScopeOpt = new Option(IteratorScope.scan.name(), "scan-time", false, "applied at scan time");
      
      OptionGroup typeGroup = new OptionGroup();
      classnameTypeOpt = new Option("class", "class-name", true, "a java class type");
      classnameTypeOpt.setArgName("name");
      aggTypeOpt = new Option("agg", "aggregator", false, "an aggregating type");
      regexTypeOpt = new Option("regex", "regular-expression", false, "a regex matching type");
      versionTypeOpt = new Option("vers", "version", false, "a versioning type");
      nolabelTypeOpt = new Option("nolabel", "no-label", false, "a no-labeling type");
      filterTypeOpt = new Option("filter", "filter", false, "a filtering type");
      
      typeGroup.addOption(classnameTypeOpt);
      typeGroup.addOption(aggTypeOpt);
      typeGroup.addOption(regexTypeOpt);
      typeGroup.addOption(versionTypeOpt);
      typeGroup.addOption(nolabelTypeOpt);
      typeGroup.addOption(filterTypeOpt);
      typeGroup.setRequired(true);
      
      o.addOption(tableOpt);
      o.addOption(priorityOpt);
      o.addOption(nameOpt);
      o.addOption(mincScopeOpt);
      o.addOption(majcScopeOpt);
      o.addOption(scanScopeOpt);
      o.addOptionGroup(typeGroup);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class SetScanIterCommand extends SetIterCommand {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      return super.execute(fullCommand, cl, shellState);
    }
    
    @Override
    protected void setTableProperties(CommandLine cl, Shell shellState, String tableName, int priority, Map<String,String> options, String classname,
        String name) throws AccumuloException, AccumuloSecurityException {
      // instead of setting table properties, just put the options
      // in a map to use at scan time
      options.put("iteratorClassName", classname);
      options.put("iteratorPriority", Integer.toString(priority));
      Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.get(tableName);
      if (tableIterators == null) {
        tableIterators = new HashMap<String,Map<String,String>>();
        shellState.scanIteratorOptions.put(tableName, tableIterators);
      }
      tableIterators.put(name, options);
    }
    
    @Override
    public String description() {
      return "sets a table-specific scan iterator for this shell session";
    }
    
    @Override
    public Options getOptions() {
      // Remove the options that specify which type of iterator this is, since
      // they are all scan iterators with this command.
      HashSet<OptionGroup> groups = new HashSet<OptionGroup>();
      Options parentOptions = super.getOptions();
      Options modifiedOptions = new Options();
      for (Iterator<?> it = parentOptions.getOptions().iterator(); it.hasNext();) {
        Option o = (Option) it.next();
        if (!IteratorScope.majc.name().equals(o.getOpt()) && !IteratorScope.minc.name().equals(o.getOpt()) && !IteratorScope.scan.name().equals(o.getOpt())) {
          modifiedOptions.addOption(o);
          OptionGroup group = parentOptions.getOptionGroup(o);
          if (group != null)
            groups.add(group);
        }
      }
      for (OptionGroup group : groups) {
        modifiedOptions.addOptionGroup(group);
      }
      return modifiedOptions;
    }
    
  }
  
  public static class DeleteScanIterCommand extends Command {
    private Option tableOpt;
    private Option nameOpt;
    private Option allOpt;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      if (!cl.hasOption(tableOpt.getOpt()))
        shellState.checkTableState();
      
      String tableName = cl.getOptionValue(tableOpt.getOpt(), shellState.tableName);
      if (tableName != null && !shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      if (cl.hasOption(allOpt.getOpt())) {
        Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.remove(tableName);
        if (tableIterators == null)
          log.info("No scan iterators set on table " + tableName);
        else
          log.info("Removed the following scan iterators from table " + tableName + ":" + tableIterators.keySet());
      } else if (cl.hasOption(nameOpt.getOpt())) {
        String name = cl.getOptionValue(nameOpt.getOpt());
        Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.get(tableName);
        if (tableIterators != null) {
          Map<String,String> options = tableIterators.remove(name);
          if (options == null)
            log.info("No iterator named " + name + " found for table " + tableName);
          else
            log.info("Removed scan iterator " + name + " from table " + tableName);
        } else {
          log.info("No iterator named " + name + " found for table " + tableName);
        }
      } else {
        throw new IllegalArgumentException("Must specify one of " + nameOpt.getArgName() + " or " + allOpt.getArgName());
      }
      
      return 0;
    }
    
    @Override
    public String description() {
      return "deletes a table-specific scan iterator so it is no longer used during this shell session";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      tableOpt = new Option(tableOption, "table", true, "tableName");
      tableOpt.setArgName("table");
      
      nameOpt = new Option("n", "name", true, "iterator to delete");
      nameOpt.setArgName("itername");
      
      allOpt = new Option("a", "all", false, "delete all for tableName");
      allOpt.setArgName("all");
      
      o.addOption(tableOpt);
      o.addOption(nameOpt);
      o.addOption(allOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class ConfigCommand extends Command {
    private Option tableOpt;
    private Option deleteOpt;
    private Option setOpt;
    private Option filterOpt;
    private Option disablePaginationOpt;
    
    private int COL1 = 8, COL2 = 7;
    private ConsoleReader reader;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      reader = shellState.reader;
      
      String tableName = cl.getOptionValue(tableOpt.getOpt());
      if (tableName != null && !shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      if (cl.hasOption(deleteOpt.getOpt())) {
        // delete property from table
        String property = cl.getOptionValue(deleteOpt.getOpt());
        if (property.contains("="))
          throw new BadArgumentException("Invalid '=' operator in delete operation.", fullCommand, fullCommand.indexOf('='));
        if (tableName != null) {
          if (!Property.isValidTablePropertyKey(property))
            log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if its there.");
          shellState.connector.tableOperations().removeProperty(tableName, property);
          log.debug("Successfully deleted table configuration option.");
        } else {
          if (!Property.isValidZooPropertyKey(property))
            log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if its there.");
          shellState.connector.instanceOperations().removeProperty(property);
          log.debug("Successfully deleted system configuration option");
        }
      } else if (cl.hasOption(setOpt.getOpt())) {
        // set property on table
        String property = cl.getOptionValue(setOpt.getOpt()), value = null;
        if (!property.contains("="))
          throw new BadArgumentException("Missing '=' operator in set operation.", fullCommand, fullCommand.indexOf(property));
        
        String pair[] = property.split("=", 2);
        property = pair[0];
        value = pair[1];
        
        if (tableName != null) {
          if (!Property.isValidTablePropertyKey(property))
            throw new BadArgumentException("Invalid per-table property.", fullCommand, fullCommand.indexOf(property));
          
          if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey()))
            new ColumnVisibility(value); // validate that it is a valid
          // expression
          
          shellState.connector.tableOperations().setProperty(tableName, property, value);
          log.debug("Successfully set table configuration option.");
        } else {
          if (!Property.isValidZooPropertyKey(property))
            throw new BadArgumentException("Property cannot be modified in zookeeper", fullCommand, fullCommand.indexOf(property));
          
          shellState.connector.instanceOperations().setProperty(property, value);
          log.debug("Successfully set system configuration option");
        }
      } else {
        // display properties
        TreeMap<String,String> systemConfig = new TreeMap<String,String>();
        for (Entry<String,String> sysEntry : AccumuloConfiguration.getSystemConfiguration(shellState.instance))
          systemConfig.put(sysEntry.getKey(), sysEntry.getValue());
        
        TreeMap<String,String> siteConfig = new TreeMap<String,String>();
        for (Entry<String,String> siteEntry : AccumuloConfiguration.getSiteConfiguration())
          siteConfig.put(siteEntry.getKey(), siteEntry.getValue());
        
        TreeMap<String,String> defaults = new TreeMap<String,String>();
        for (Entry<String,String> defaultEntry : AccumuloConfiguration.getDefaultConfiguration())
          defaults.put(defaultEntry.getKey(), defaultEntry.getValue());
        
        Iterable<Entry<String,String>> acuconf = AccumuloConfiguration.getSystemConfiguration();
        if (tableName != null)
          acuconf = shellState.connector.tableOperations().getProperties(tableName);
        
        for (Entry<String,String> propEntry : acuconf) {
          String key = propEntry.getKey();
          // only show properties with similar names to that
          // specified, or all of them if none specified
          if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt())))
            continue;
          if (tableName != null && !Property.isValidTablePropertyKey(key))
            continue;
          COL2 = Math.max(COL2, propEntry.getKey().length() + 3);
        }
        
        ArrayList<String> output = new ArrayList<String>();
        printConfHeader(output);
        
        for (Entry<String,String> propEntry : acuconf) {
          String key = propEntry.getKey();
          
          // only show properties with similar names to that
          // specified, or all of them if none specified
          if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt())))
            continue;
          
          if (tableName != null && !Property.isValidTablePropertyKey(key))
            continue;
          
          String siteVal = siteConfig.get(key);
          String sysVal = systemConfig.get(key);
          String curVal = propEntry.getValue();
          String dfault = defaults.get(key);
          boolean printed = false;
          
          if (dfault != null && key.toLowerCase().contains("password")) {
            dfault = curVal = curVal.replaceAll(".", "*");
          }
          if (sysVal != null) {
            if (defaults.containsKey(key)) {
              printConfLine(output, "default", key, dfault);
              printed = true;
            }
            if (!defaults.containsKey(key) || !defaults.get(key).equals(siteVal)) {
              printConfLine(output, "site", printed ? "   @override" : key, siteVal == null ? "" : siteVal);
              printed = true;
            }
            if (!siteConfig.containsKey(key) || !siteVal.equals(sysVal)) {
              printConfLine(output, "system", printed ? "   @override" : key, sysVal == null ? "" : sysVal);
              printed = true;
            }
          }
          
          // show per-table value only if it is different (overridden)
          if (tableName != null && !curVal.equals(sysVal))
            printConfLine(output, "table", printed ? "   @override" : key, curVal);
        }
        printConfFooter(output);
        shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
      }
      return 0;
    }
    
    private void printConfHeader(ArrayList<String> output) {
      printConfFooter(output);
      output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", "SCOPE", "NAME", "VALUE"));
      printConfFooter(output);
    }
    
    private void printConfLine(ArrayList<String> output, String s1, String s2, String s3) {
      if (s2.length() < COL2)
        s2 += " " + repeat(".", COL2 - s2.length() - 1);
      output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", s1, s2,
          s3.replace("\n", "\n" + repeat(" ", COL1 + 1) + "|" + repeat(" ", COL2 + 2) + "|" + " ")));
    }
    
    private void printConfFooter(ArrayList<String> output) {
      int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTermwidth() - COL1 - COL2 - 6));
      output.add(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%-" + col3 + "s", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    }
    
    @Override
    public String description() {
      return "prints system properties and table specific properties";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      OptionGroup og = new OptionGroup();
      
      tableOpt = new Option(tableOption, "table", true, "display/set/delete properties for specified table");
      deleteOpt = new Option("d", "delete", true, "delete a per-table property");
      setOpt = new Option("s", "set", true, "set a per-table property");
      filterOpt = new Option("f", "filter", true, "show only properties that contain this string");
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      
      tableOpt.setArgName("table");
      deleteOpt.setArgName("property");
      setOpt.setArgName("property=value");
      filterOpt.setArgName("string");
      
      og.addOption(deleteOpt);
      og.addOption(setOpt);
      og.addOption(filterOpt);
      
      o.addOption(tableOpt);
      o.addOptionGroup(og);
      o.addOption(disablePaginationOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class UserCommand extends Command {
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      // save old credentials and connection in case of failure
      AuthInfo prevCreds = shellState.credentials;
      Connector prevConn = shellState.connector;
      
      boolean revertCreds = false;
      String user = cl.getArgs()[0];
      byte[] pass;
      
      // We can't let the wrapping try around the execute method deal
      // with the exceptions because we have to do something if one
      // of these methods fails
      try {
        String p = shellState.reader.readLine("Enter password for user " + user + ": ", '*');
        if (p == null) {
          shellState.reader.printNewline();
          return 0;
        } // user canceled
        pass = p.getBytes();
        shellState.credentials = new AuthInfo(user, pass, shellState.connector.getInstance().getInstanceID());
        shellState.connector = shellState.connector.getInstance().getConnector(user, pass);
      } catch (AccumuloException e) {
        revertCreds = true;
        throw e;
      } catch (AccumuloSecurityException e) {
        revertCreds = true;
        throw e;
      } finally {
        // revert to saved credentials if failure occurred
        if (revertCreds) {
          shellState.credentials = prevCreds;
          shellState.connector = prevConn;
        }
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "switches to the specified user";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> special) {
      registerCompletionForUsers(root, special);
    }
    
    @Override
    public String usage() {
      return getName() + " <username>";
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class ScanCommand extends Command {
    private Option scanOptAuths;
    private Option scanOptStartRow;
    private Option scanOptEndRow;
    private Option scanOptColumns;
    protected Option timestampOpt;
    private Option disablePaginationOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException, ParseException {
      shellState.checkTableState();
      
      // handle first argument, if present, the authorizations list to
      // scan with
      Authorizations auths = getAuths(cl, shellState);
      Scanner scanner = shellState.connector.createScanner(shellState.tableName, auths);
      
      // handle session-specific scan iterators
      setScanIterators(shellState, scanner);
      
      // handle remaining optional arguments
      scanner.setRange(getRange(cl));
      
      // handle columns
      fetchColumns(cl, scanner);
      
      // output the records
      printRecords(cl, shellState, scanner);
      return 0;
    }
    
    protected void setScanIterators(Shell shellState, Scanner scanner) throws IOException {
      Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.get(shellState.tableName);
      if (tableIterators == null)
        return;
      
      for (Entry<String,Map<String,String>> entry : tableIterators.entrySet()) {
        Map<String,String> options = new HashMap<String,String>(entry.getValue());
        String className = options.remove("iteratorClassName");
        int priority = Integer.parseInt(options.remove("iteratorPriority"));
        String name = entry.getKey();
        
        log.debug("Setting scan iterator " + name + " at priority " + priority + " using class name " + className);
        scanner.setScanIterators(priority, className, name);
        for (Entry<String,String> option : options.entrySet()) {
          log.debug("Setting option for " + name + ": " + option.getKey() + "=" + option.getValue());
          scanner.setScanIteratorOption(name, option.getKey(), option.getValue());
        }
      }
    }
    
    protected void printRecords(CommandLine cl, Shell shellState, Iterable<Entry<Key,Value>> scanner) throws IOException {
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
    }
    
    protected void fetchColumns(CommandLine cl, ScannerBase scanner) {
      if (cl.hasOption(scanOptColumns.getOpt())) {
        for (String a : cl.getOptionValue(scanOptColumns.getOpt()).split(",")) {
          String sa[] = a.split(":", 2);
          if (sa.length == 1)
            scanner.fetchColumnFamily(new Text(a));
          else
            scanner.fetchColumn(new Text(sa[0]), new Text(sa[1]));
        }
      }
    }
    
    protected Range getRange(CommandLine cl) {
      Text startRow = cl.hasOption(scanOptStartRow.getOpt()) ? new Text(cl.getOptionValue(scanOptStartRow.getOpt())) : null;
      Text endRow = cl.hasOption(scanOptEndRow.getOpt()) ? new Text(cl.getOptionValue(scanOptEndRow.getOpt())) : null;
      Range r = new Range(startRow, endRow);
      return r;
    }
    
    protected Authorizations getAuths(CommandLine cl, Shell shellState) throws AccumuloSecurityException, AccumuloException {
      String user = shellState.connector.whoami();
      Authorizations auths = shellState.connector.securityOperations().getUserAuthorizations(user);
      if (cl.hasOption(scanOptAuths.getOpt())) {
        auths = parseAuthorizations(cl.getOptionValue(scanOptAuths.getOpt()));
      }
      return auths;
    }
    
    @Override
    public String description() {
      return "scans the table, and displays the resulting records";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations (all user auths are used if this argument is not specified)");
      scanOptStartRow = new Option("b", "begin-row", true, "begin row (inclusive)");
      scanOptEndRow = new Option("e", "end-row", true, "end row (inclusive)");
      scanOptColumns = new Option("c", "columns", true, "comma-separated columns");
      timestampOpt = new Option("st", "show-timestamps", false, "enables displaying timestamps");
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      
      scanOptAuths.setArgName("comma-separated-authorizations");
      scanOptStartRow.setArgName("start-row");
      scanOptEndRow.setArgName("end-row");
      scanOptColumns.setArgName("{<columnfamily>[:<columnqualifier>]}");
      
      o.addOption(scanOptAuths);
      o.addOption(scanOptStartRow);
      o.addOption(scanOptEndRow);
      o.addOption(scanOptColumns);
      o.addOption(timestampOpt);
      o.addOption(disablePaginationOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class DeleteManyCommand extends ScanCommand {
    private Option forceOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException, ParseException {
      shellState.checkTableState();
      
      // handle first argument, if present, the authorizations list to
      // scan with
      Authorizations auths = getAuths(cl, shellState);
      final Scanner scanner = shellState.connector.createScanner(shellState.tableName, auths);
      
      scanner.setScanIterators(Integer.MAX_VALUE, SortedKeyIterator.class.getName(), "NOVALUE");
      
      // handle remaining optional arguments
      scanner.setRange(getRange(cl));
      
      // handle columns
      fetchColumns(cl, scanner);
      
      // output / delete the records
      BatchWriter writer = shellState.connector.createBatchWriter(shellState.tableName, 1024 * 1024, 1000L, 4);
      shellState.printLines(new DeleterFormatter(writer, scanner, cl.hasOption(timestampOpt.getOpt()), shellState, cl.hasOption(forceOpt.getOpt())), false);
      return 0;
    }
    
    @Override
    public String description() {
      return "scans a table and deletes the resulting records";
    }
    
    @Override
    public Options getOptions() {
      forceOpt = new Option("f", "force", false, "forces deletion without prompting");
      Options opts = super.getOptions();
      opts.addOption(forceOpt);
      return opts;
    }
    
  }
  
  public static class GrepCommand extends ScanCommand {
    
    private Option numThreadsOpt;
    
    protected void setUpIterator(int prio, String name, String term, BatchScanner scanner) throws IOException {
      if (prio < 0)
        throw new IllegalArgumentException("Priority < 0 " + prio);
      
      scanner.setScanIterators(prio, GrepIterator.class.getName(), name);
      scanner.setScanIteratorOption(name, "term", term);
    }
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException, MissingArgumentException {
      shellState.checkTableState();
      
      if (cl.getArgList().isEmpty())
        throw new MissingArgumentException("No terms specified");
      
      // handle first argument, if present, the authorizations list to
      // scan with
      int numThreads = 20;
      if (cl.hasOption(numThreadsOpt.getOpt())) {
        numThreads = Integer.parseInt(cl.getOptionValue(numThreadsOpt.getOpt()));
      }
      Authorizations auths = getAuths(cl, shellState);
      BatchScanner scanner = shellState.connector.createBatchScanner(shellState.tableName, auths, numThreads);
      scanner.setRanges(Collections.singletonList(getRange(cl)));
      
      for (int i = 0; i < cl.getArgs().length; i++)
        setUpIterator(Integer.MAX_VALUE, "grep" + i, cl.getArgs()[i], scanner);
      
      try {
        // handle columns
        fetchColumns(cl, scanner);
        
        // output the records
        printRecords(cl, shellState, scanner);
      } finally {
        scanner.close();
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "searches a table for a substring, in parallel, on the server side";
    }
    
    @Override
    public Options getOptions() {
      Options opts = super.getOptions();
      numThreadsOpt = new Option("t", "num-threads", true, "num threads");
      opts.addOption(numThreadsOpt);
      return opts;
    }
    
    @Override
    public String usage() {
      return getName() + " <term>{ <term>}";
    }
    
    @Override
    public int numArgs() {
      return NO_FIXED_ARG_LENGTH_CHECK;
    }
  }
  
  public static class EGrepCommand extends GrepCommand {
    @Override
    protected void setUpIterator(int prio, String name, String term, BatchScanner scanner) throws IOException {
      if (prio < 0)
        throw new IllegalArgumentException("Priority < 0 " + prio);
      
      scanner.setScanIterators(prio, RegExIterator.class.getName(), name);
      scanner.setScanIteratorOption(name, RegExFilter.ROW_REGEX, term);
      scanner.setScanIteratorOption(name, RegExFilter.COLF_REGEX, term);
      scanner.setScanIteratorOption(name, RegExFilter.COLQ_REGEX, term);
      scanner.setScanIteratorOption(name, RegExFilter.VALUE_REGEX, term);
      scanner.setScanIteratorOption(name, RegExFilter.OR_FIELDS, "true");
    }
    
    @Override
    public String description() {
      return "egreps a table in parallel on the server side (uses java regex)";
    }
    
    @Override
    public String usage() {
      return getName() + " <regex>{ <regex>}";
    }
  }
  
  public static class DeleteCommand extends Command {
    private Option deleteOptAuths;
    private Option timestampOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException, ConstraintViolationException {
      shellState.checkTableState();
      
      Mutation m = new Mutation(new Text(cl.getArgs()[0]));
      
      if (cl.hasOption(deleteOptAuths.getOpt())) {
        ColumnVisibility le = new ColumnVisibility(cl.getOptionValue(deleteOptAuths.getOpt()));
        if (cl.hasOption(timestampOpt.getOpt()))
          m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le, Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
        else
          m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le);
      } else if (cl.hasOption(timestampOpt.getOpt()))
        m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())));
      else
        m.putDelete(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]));
      
      BatchWriter bw = shellState.connector.createBatchWriter(shellState.tableName, m.estimatedMemoryUsed() + 0L, 0L, 1);
      bw.addMutation(m);
      bw.close();
      return 0;
    }
    
    @Override
    public String description() {
      return "deletes a record from a table";
    }
    
    @Override
    public String usage() {
      return getName() + " <row> <colfamily> <colqualifier>";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      deleteOptAuths = new Option("l", "authorization-label", true, "formatted authorization label expression");
      deleteOptAuths.setArgName("expression");
      o.addOption(deleteOptAuths);
      
      timestampOpt = new Option("t", "timestamp", true, "timestamp to use for insert");
      timestampOpt.setArgName("timestamp");
      o.addOption(timestampOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 3;
    }
  }
  
  public static class FlushCommand extends Command {
    private Option optTablePattern;
    private Option optTableName;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      // populate the tablesToFlush set with the tables you want to flush
      SortedSet<String> tablesToFlush = new TreeSet<String>();
      if (cl.hasOption(optTablePattern.getOpt())) {
        for (String table : shellState.connector.tableOperations().list())
          if (table.matches(cl.getOptionValue(optTablePattern.getOpt())))
            tablesToFlush.add(table);
      } else if (cl.hasOption(optTableName.getOpt())) {
        tablesToFlush.add(cl.getOptionValue(optTableName.getOpt()));
      }
      
      if (tablesToFlush.isEmpty())
        log.warn("No tables found that match your criteria");
      
      // flush the tables
      for (String tableName : tablesToFlush) {
        if (!shellState.connector.tableOperations().exists(tableName))
          throw new TableNotFoundException(null, tableName, null);
        flush(shellState, tableName);
      }
      return 0;
    }
    
    protected void flush(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
      shellState.connector.tableOperations().flush(tableName);
      log.info("Flush of table " + tableName + " initiated...");
      if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
        log.info("  May need to flush " + Constants.METADATA_TABLE_NAME + " table multiple times.");
        log.info("  Flushing " + Constants.METADATA_TABLE_NAME + " causes writes to itself and");
        log.info("  minor compactions, which also cause writes to itself.");
        log.info("  Check the monitor web page and give it time to settle.");
      }
    }
    
    @Override
    public String description() {
      return "makes a best effort to flush tables from memory to disk";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      optTablePattern = new Option("p", "pattern", true, "regex pattern of table names to flush");
      optTableName = new Option(tableOption, "table", true, "name of a table to flush");
      
      optTablePattern.setArgName("pattern");
      optTableName.setArgName("tableName");
      
      OptionGroup opg = new OptionGroup();
      opg.addOption(optTablePattern);
      opg.addOption(optTableName);
      opg.setRequired(true);
      
      o.addOptionGroup(opg);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class OfflineCommand extends FlushCommand {
    
    @Override
    public String description() {
      return "starts the process of taking table offline";
    }
    
    protected void flush(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
      if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
        log.info("  You cannot take the " + Constants.METADATA_TABLE_NAME + " offline.");
      } else {
        log.info("Attempting to begin taking " + tableName + " offline");
        shellState.connector.tableOperations().offline(tableName);
      }
    }
  }
  
  public static class OnlineCommand extends FlushCommand {
    
    @Override
    public String description() {
      return "starts the process of putting a table online";
    }
    
    protected void flush(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
      if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
        log.info("  The " + Constants.METADATA_TABLE_NAME + " is always online.");
      } else {
        log.info("Attempting to begin bringing " + tableName + " online");
        shellState.connector.tableOperations().online(tableName);
      }
    }
  }
  
  public static class AddSplitsCommand extends Command {
    private Option optTableName;
    private Option optSplitsFile;
    private Option base64Opt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        MissingArgumentException, FileNotFoundException {
      String tableName = cl.getOptionValue(optTableName.getOpt());
      boolean decode = cl.hasOption(base64Opt.getOpt());
      
      TreeSet<Text> splits = new TreeSet<Text>();
      
      if (cl.hasOption(optSplitsFile.getOpt())) {
        String f = cl.getOptionValue(optSplitsFile.getOpt());
        
        String line;
        java.util.Scanner file = new java.util.Scanner(new File(f));
        while (file.hasNextLine()) {
          line = file.nextLine();
          if (!line.isEmpty())
            splits.add(decode ? new Text(Base64.decodeBase64(line.getBytes())) : new Text(line));
        }
      } else {
        if (cl.getArgList().isEmpty())
          throw new MissingArgumentException("No split points specified");
        
        for (String s : cl.getArgs()) {
          splits.add(new Text(s));
        }
      }
      
      if (!shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      shellState.connector.tableOperations().addSplits(tableName, splits);
      
      return 0;
    }
    
    @Override
    public String description() {
      return "add split points to an existing table";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      optTableName = new Option(tableOption, "table", true, "name of a table to add split points to");
      optSplitsFile = new Option("sf", "splits-file", true, "file with newline separated list of rows to add to table");
      optTableName.setArgName("tableName");
      optTableName.setRequired(true);
      optSplitsFile.setArgName("filename");
      base64Opt = new Option("b64", "base64encoded", false, "decode encoded split points");
      
      o.addOption(optTableName);
      o.addOption(optSplitsFile);
      o.addOption(base64Opt);
      return o;
    }
    
    @Override
    public String usage() {
      return getName() + " [<split>{ <split>} ]";
    }
    
    @Override
    public int numArgs() {
      return NO_FIXED_ARG_LENGTH_CHECK;
    }
  }
  
  public static class SelectCommand extends Command {
    private Option selectOptAuths;
    private Option timestampOpt;
    private Option disablePaginationOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      shellState.checkTableState();
      
      Authorizations authorizations = cl.hasOption(selectOptAuths.getOpt()) ? parseAuthorizations(cl.getOptionValue(selectOptAuths.getOpt()))
          : Constants.NO_AUTHS;
      Scanner scanner = shellState.connector.createScanner(shellState.tableName.toString(), authorizations);
      
      Key key = new Key(new Text(cl.getArgs()[0]), new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]));
      scanner.setRange(new Range(key, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL)));
      
      // output the records
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
      return 0;
    }
    
    @Override
    public String description() {
      return "scans for and displays a single record";
    }
    
    @Override
    public String usage() {
      return getName() + " <row> <columnfamily> <columnqualifier>";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      selectOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations");
      selectOptAuths.setArgName("comma-separated-authorizations");
      timestampOpt = new Option("st", "show-timestamps", false, "enables displaying timestamps");
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      o.addOption(selectOptAuths);
      o.addOption(timestampOpt);
      o.addOption(disablePaginationOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 3;
    }
  }
  
  public static class SelectrowCommand extends Command {
    private Option selectrowOptAuths;
    private Option timestampOpt;
    private Option disablePaginationOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      shellState.checkTableState();
      
      Authorizations auths = cl.hasOption(selectrowOptAuths.getOpt()) ? parseAuthorizations(cl.getOptionValue(selectrowOptAuths.getOpt())) : Constants.NO_AUTHS;
      Scanner scanner = shellState.connector.createScanner(shellState.tableName.toString(), auths);
      scanner.setRange(new Range(new Text(cl.getArgs()[0])));
      
      // output the records
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
      return 0;
    }
    
    @Override
    public String description() {
      return "scans a single row and displays all resulting records";
    }
    
    @Override
    public String usage() {
      return getName() + " <row>";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      selectrowOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations");
      selectrowOptAuths.setArgName("comma-separated-authorizations");
      timestampOpt = new Option("st", "show-timestamps", false, "enables displaying timestamps");
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      o.addOption(selectrowOptAuths);
      o.addOption(timestampOpt);
      o.addOption(disablePaginationOpt);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class InsertCommand extends Command {
    private Option insertOptAuths;
    private Option timestampOpt;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException, ConstraintViolationException {
      shellState.checkTableState();
      
      Mutation m = new Mutation(new Text(cl.getArgs()[0]));
      
      if (cl.hasOption(insertOptAuths.getOpt())) {
        ColumnVisibility le = new ColumnVisibility(cl.getOptionValue(insertOptAuths.getOpt()));
        log.debug("Authorization label will be set to: " + le.toString());
        
        if (cl.hasOption(timestampOpt.getOpt()))
          m.put(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le, Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())),
              new Value(cl.getArgs()[3].getBytes()));
        else
          m.put(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), le, new Value(cl.getArgs()[3].getBytes()));
      } else if (cl.hasOption(timestampOpt.getOpt()))
        m.put(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), Long.parseLong(cl.getOptionValue(timestampOpt.getOpt())),
            new Value(cl.getArgs()[3].getBytes()));
      else
        m.put(new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]), new Value(cl.getArgs()[3].getBytes()));
      
      BatchWriter bw = shellState.connector.createBatchWriter(shellState.tableName, m.estimatedMemoryUsed() + 0L, 0L, 1);
      bw.addMutation(m);
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        ArrayList<String> lines = new ArrayList<String>();
        if (e.getAuthorizationFailures().isEmpty() == false)
          lines.add("	Authorization Failures:");
        for (KeyExtent extent : e.getAuthorizationFailures()) {
          lines.add("		" + extent);
        }
        if (e.getConstraintViolationSummaries().isEmpty() == false)
          lines.add("	Constraint Failures:");
        for (ConstraintViolationSummary cvs : e.getConstraintViolationSummaries()) {
          lines.add("		" + cvs.toString());
        }
        shellState.printLines(lines.iterator(), false);
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "inserts a record";
    }
    
    @Override
    public String usage() {
      return getName() + " <row> <colfamily> <colqualifier> <value>";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      insertOptAuths = new Option("l", "authorization-label", true, "formatted authorization label expression");
      insertOptAuths.setArgName("expression");
      o.addOption(insertOptAuths);
      
      timestampOpt = new Option("t", "timestamp", true, "timestamp to use for insert");
      timestampOpt.setArgName("timestamp");
      o.addOption(timestampOpt);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 4;
    }
  }
  
  public static class DropTableCommand extends DeleteTableCommand {}
  
  public static class DeleteTableCommand extends Command {
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        IOException {
      String tableName = cl.getArgs()[0];
      if (shellState.tableName.equals(tableName))
        shellState.tableName = "";
      shellState.connector.tableOperations().delete(tableName);
      return 0;
    }
    
    @Override
    public String description() {
      return "deletes a table";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> special) {
      registerCompletionForTables(root, special);
    }
    
    @Override
    public String usage() {
      return getName() + " <tableName>";
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class CreateTableCommand extends Command {
    private Option createTableOptCopySplits;
    private Option createTableOptCopyConfig;
    private Option createTableOptSplit;
    private Option createTableOptAgg;
    private Option createTableOptTimeLogical;
    private Option createTableOptTimeMillis;
    private Option createTableNoDefaultIters;
    private Option base64Opt;
    public static String testTable;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableExistsException,
        TableNotFoundException, IOException {
      
      String testTableName = cl.getArgs()[0];
      
      if (!testTableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
        shellState.reader.printString("Only letters, numbers and underscores are allowed for use in table names. \n");
        throw new IllegalArgumentException();
      }
      
      String tableName = cl.getArgs()[0];
      if (shellState.connector.tableOperations().exists(tableName))
        throw new TableExistsException(null, tableName, null);
      
      SortedSet<Text> partitions = new TreeSet<Text>();
      List<AggregatorConfiguration> aggregators = new ArrayList<AggregatorConfiguration>();
      boolean decode = cl.hasOption(base64Opt.getOpt());
      
      if (cl.hasOption(createTableOptAgg.getOpt())) {
        String agg = cl.getOptionValue(createTableOptAgg.getOpt());
        
        EscapeTokenizer st = new EscapeTokenizer(agg, "=,");
        if (st.count() % 2 != 0) {
          printHelp();
          return 0;
        }
        Iterator<String> iter = st.iterator();
        while (iter.hasNext()) {
          String col = iter.next();
          String className = iter.next();
          
          EscapeTokenizer colToks = new EscapeTokenizer(col, ":");
          Iterator<String> tokIter = colToks.iterator();
          Text cf = null, cq = null;
          if (colToks.count() < 1 || colToks.count() > 2)
            throw new BadArgumentException("column must be in the format cf[:cq]", fullCommand, fullCommand.indexOf(col));
          cf = new Text(tokIter.next());
          if (colToks.count() == 2)
            cq = new Text(tokIter.next());
          
          aggregators.add(new AggregatorConfiguration(cf, cq, className));
        }
      }
      if (cl.hasOption(createTableOptSplit.getOpt())) {
        String f = cl.getOptionValue(createTableOptSplit.getOpt());
        
        String line;
        java.util.Scanner file = new java.util.Scanner(new File(f));
        while (file.hasNextLine()) {
          line = file.nextLine();
          if (!line.isEmpty())
            partitions.add(decode ? new Text(Base64.decodeBase64(line.getBytes())) : new Text(line));
        }
      } else if (cl.hasOption(createTableOptCopySplits.getOpt())) {
        String oldTable = cl.getOptionValue(createTableOptCopySplits.getOpt());
        if (!shellState.connector.tableOperations().exists(oldTable))
          throw new TableNotFoundException(null, oldTable, null);
        partitions.addAll(shellState.connector.tableOperations().getSplits(oldTable));
      }
      
      if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
        String oldTable = cl.getOptionValue(createTableOptCopyConfig.getOpt());
        if (!shellState.connector.tableOperations().exists(oldTable))
          throw new TableNotFoundException(null, oldTable, null);
      }
      
      TimeType timeType = TimeType.MILLIS;
      if (cl.hasOption(createTableOptTimeLogical.getOpt()))
        timeType = TimeType.LOGICAL;
      
      shellState.connector.tableOperations().create(tableName, timeType); // create
      // table
      
      shellState.connector.tableOperations().addSplits(tableName, partitions);
      shellState.connector.tableOperations().addAggregators(tableName, aggregators);
      
      shellState.tableName = tableName; // switch shell to new table
      // context
      
      if (cl.hasOption(createTableNoDefaultIters.getOpt())) {
        List<AggregatorConfiguration> empty = Collections.emptyList();
        for (String key : IteratorUtil.generateInitialTableProperties(empty).keySet())
          shellState.connector.tableOperations().removeProperty(tableName, key);
      }
      
      // Copy options if flag was set
      if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
        if (shellState.connector.tableOperations().exists(tableName)) {
          for (Entry<String,String> entry : AccumuloConfiguration.getTableConfiguration(shellState.connector.getInstance().getInstanceID(),
              shellState.connector.tableOperations().tableIdMap().get(cl.getOptionValue(createTableOptCopyConfig.getOpt()))))
            if (Property.isValidTablePropertyKey(entry.getKey()))
              shellState.connector.tableOperations().setProperty(tableName, entry.getKey(), entry.getValue());
        }
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "creates a new table, with optional aggregators and optionally pre-split";
    }
    
    @Override
    public String usage() {
      return getName() + " <tableName>";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      createTableOptCopyConfig = new Option("cc", "copy-config", true, "table to copy configuration from");
      createTableOptCopySplits = new Option("cs", "copy-splits", true, "table to copy current splits from");
      createTableOptSplit = new Option("sf", "splits-file", true, "file with newline separated list of rows to create a pre-split table");
      createTableOptAgg = new Option("a", "aggregator", true, "comma separated column=aggregator");
      createTableOptTimeLogical = new Option("tl", "time-logical", false, "use logical time");
      createTableOptTimeMillis = new Option("tm", "time-millis", false, "use time in milliseconds");
      createTableNoDefaultIters = new Option("ndi", "no-default-iterators", false, "prevents creation of the normal default iterator set");
      
      createTableOptCopyConfig.setArgName("table");
      createTableOptCopySplits.setArgName("table");
      createTableOptSplit.setArgName("filename");
      createTableOptAgg.setArgName("{<columnfamily>[:<columnqualifier>]=<aggregation_class>}");
      
      // Splits and CopySplits are put in an optionsgroup to make them
      // mutually exclusive
      OptionGroup splitOrCopySplit = new OptionGroup();
      splitOrCopySplit.addOption(createTableOptSplit);
      splitOrCopySplit.addOption(createTableOptCopySplits);
      
      OptionGroup timeGroup = new OptionGroup();
      timeGroup.addOption(createTableOptTimeLogical);
      timeGroup.addOption(createTableOptTimeMillis);
      
      base64Opt = new Option("b64", "base64encoded", false, "decode encoded split points");
      o.addOption(base64Opt);
      
      o.addOptionGroup(splitOrCopySplit);
      o.addOptionGroup(timeGroup);
      o.addOption(createTableOptSplit);
      o.addOption(createTableOptAgg);
      o.addOption(createTableOptCopyConfig);
      o.addOption(createTableNoDefaultIters);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class RenameTableCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
        TableExistsException {
      shellState.connector.tableOperations().rename(cl.getArgs()[0], cl.getArgs()[1]);
      if (shellState.tableName.equals(cl.getArgs()[0]))
        shellState.tableName = cl.getArgs()[1];
      return 0;
    }
    
    @Override
    public String usage() {
      return getName() + " <current table name> <new table name>";
    }
    
    @Override
    public String description() {
      return "rename a table";
    }
    
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionForTables(root, completionSet);
    }
    
    @Override
    public int numArgs() {
      return 2;
    }
  }
  
  public static class TablesCommand extends Command {
    private Option tableIdOption;
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, IOException {
      if (cl.hasOption(tableIdOption.getOpt())) {
        Map<String,String> tableIds = shellState.connector.tableOperations().tableIdMap();
        for (String tableName : shellState.connector.tableOperations().list())
          shellState.reader.printString(String.format("%-15s => %10s\n", tableName, tableIds.get(tableName)));
      } else {
        for (String table : shellState.connector.tableOperations().list())
          shellState.reader.printString(table + "\n");
      }
      return 0;
    }
    
    @Override
    public String description() {
      return "displays a list of all existing tables";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      tableIdOption = new Option("l", "list-ids", false, "display internal table ids along with the table name");
      o.addOption(tableIdOption);
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class TableCommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      String tableName = cl.getArgs()[0];
      if (!shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      shellState.tableName = tableName;
      return 0;
    }
    
    @Override
    public String description() {
      return "switches to the specified table";
    }
    
    @Override
    public void registerCompletion(Token root, Map<CompletionSet,Set<String>> special) {
      registerCompletionForTables(root, special);
    }
    
    @Override
    public String usage() {
      return getName() + " <tableName>";
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
  }
  
  public static class WhoAmICommand extends Command {
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      shellState.reader.printString(shellState.connector.whoami() + "\n");
      return 0;
    }
    
    @Override
    public String description() {
      return "reports the current user name";
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class ClsCommand extends ClearCommand {}
  
  public static class ClearCommand extends Command {
    @Override
    public String description() {
      return "clears the screen";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      // custom clear screen, so I don't have to redraw the prompt twice
      if (!shellState.reader.getTerminal().isANSISupported())
        throw new IOException("Terminal does not support ANSI commands");
      
      // send the ANSI code to clear the screen
      shellState.reader.printString(((char) 27) + "[2J");
      shellState.reader.flushConsole();
      
      // then send the ANSI code to go to position 1,1
      shellState.reader.printString(((char) 27) + "[1;1H");
      shellState.reader.flushConsole();
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
  }
  
  public static class FormatterCommand extends Command {
    private Option resetOption, formatterClassOption, listClassOption;
    
    @Override
    public String description() {
      return "specifies a formatter to use for displaying database entries";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      if (cl.hasOption(resetOption.getOpt()))
        shellState.formatterClass = DefaultFormatter.class;
      else if (cl.hasOption(formatterClassOption.getOpt()))
        shellState.formatterClass = AccumuloClassLoader.loadClass(cl.getOptionValue(formatterClassOption.getOpt()), Formatter.class);
      else if (cl.hasOption(listClassOption.getOpt()))
        shellState.reader.printString(shellState.formatterClass.getName() + "\n");
      return 0;
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      OptionGroup formatGroup = new OptionGroup();
      
      resetOption = new Option("r", "reset", false, "reset to default formatter");
      formatterClassOption = new Option("f", "formatter", true, "fully qualified name of formatter class to use");
      formatterClassOption.setArgName("className");
      listClassOption = new Option("l", "list", false, "display the current formatter");
      formatGroup.addOption(resetOption);
      formatGroup.addOption(formatterClassOption);
      formatGroup.addOption(listClassOption);
      formatGroup.setRequired(true);
      
      o.addOptionGroup(formatGroup);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
    
  }
  
  public static class ExecfileCommand extends Command {
    private Option verboseOption;
    
    @Override
    public String description() {
      return "specifies a file containing accumulo commands to execute";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      java.util.Scanner scanner = new java.util.Scanner(new File(cl.getArgs()[0]));
      while (scanner.hasNextLine())
        shellState.execCommand(scanner.nextLine(), true, cl.hasOption(verboseOption.getOpt()));
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 1;
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      verboseOption = new Option("v", "verbose", false, "displays command prompt as commands are executed");
      opts.addOption(verboseOption);
      return opts;
    }
  }
  
  public static class CompactCommand extends FlushCommand {
    private Option overrideOpt;
    SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz");
    boolean override = false;
    
    @Override
    public String description() {
      return "sets all tablets for a table to major compact as soon as possible (based on current time)";
    }
    
    protected void flush(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
      // compact the tables
      Date now = new Date(System.currentTimeMillis());
      String nowStr = dateParser.format(now);
      try {
        if (!override) {
          Iterable<Entry<String,String>> iter = shellState.connector.tableOperations().getProperties(tableName);
          for (Entry<String,String> prop : iter) {
            if (prop.getKey().equals(Property.TABLE_MAJC_COMPACTALL_AT.getKey())) {
              if (dateParser.parse(prop.getValue()).after(now)) {
                log.error("Date will override a scheduled future compaction for table (" + tableName + "). Use --" + overrideOpt.getLongOpt());
                return;
              } else {
                break;
              }
            }
          }
        }
        shellState.connector.tableOperations().flush(tableName);
        shellState.connector.tableOperations().setProperty(tableName, Property.TABLE_MAJC_COMPACTALL_AT.getKey(), nowStr);
        log.info("Compaction of table " + tableName + " scheduled for " + nowStr);
      } catch (Exception ex) {
        throw new AccumuloException(ex);
      }
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      override = cl.hasOption(overrideOpt.getLongOpt());
      return super.execute(fullCommand, cl, shellState);
    }
    
    @Override
    public Options getOptions() {
      Options opts = super.getOptions();
      overrideOpt = new Option(null, "override", false, "override a future scheduled compaction");
      opts.addOption(overrideOpt);
      return opts;
    }
  }
  
  public static class InfoCommand extends AboutCommand {}
  
  public static class AboutCommand extends Command {
    private Option verboseOption;
    
    @Override
    public String description() {
      return "displays information about this program";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException {
      shellState.printInfo();
      if (cl.hasOption(verboseOption.getOpt()))
        shellState.printVerboseInfo();
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      verboseOption = new Option("v", "verbose", false, "displays details session information");
      opts.addOption(verboseOption);
      return opts;
    }
  }
  
  public static class HiddenCommand extends Command {
    private static Random rand = new SecureRandom();
    
    @Override
    public String description() {
      return "The first rule of accumulo is: \"You don't talk about accumulo.\"";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      if (rand.nextInt(10) == 0) {
        shellState.reader.beep();
        shellState.reader.printNewline();
        shellState.reader.printString("Sortacus lives!");
        shellState.reader.printNewline();
      } else
        throw new ShellCommandException(ErrorCode.UNRECOGNIZED_COMMAND, getName());
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return NO_FIXED_ARG_LENGTH_CHECK;
    }
    
    @Override
    public String getName() {
      return "\0\0\0\0";
    }
  }
  
  public static class GetGroupsCommand extends Command {
    
    private Option tableOpt;
    
    @Override
    public String description() {
      return "gets the locality groups for a given table";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      
      String tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      Map<String,Set<Text>> groups = shellState.connector.tableOperations().getLocalityGroups(tableName);
      
      for (Entry<String,Set<Text>> entry : groups.entrySet())
        shellState.reader.printString(entry.getKey() + "=" + LocalityGroupUtil.encodeColumnFamilies(entry.getValue()) + "\n");
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      
      tableOpt = new Option(tableOption, "table", true, "get locality groups for specified table");
      tableOpt.setArgName("table");
      tableOpt.setRequired(true);
      opts.addOption(tableOpt);
      return opts;
    }
    
  }
  
  public static class SetGroupsCommand extends Command {
    
    private Option tableOpt;
    
    @Override
    public String description() {
      return "sets the locality groups for a given table (for binary or commas, use Java API)";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      
      String tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.connector.tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      HashMap<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
      
      for (String arg : cl.getArgs()) {
        String sa[] = arg.split("=", 2);
        if (sa.length < 2)
          throw new IllegalArgumentException("Missing '='");
        String group = sa[0];
        HashSet<Text> colFams = new HashSet<Text>();
        
        for (String family : sa[1].split(",")) {
          colFams.add(new Text(family));
        }
        
        groups.put(group, colFams);
      }
      
      shellState.connector.tableOperations().setLocalityGroups(tableName, groups);
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return NO_FIXED_ARG_LENGTH_CHECK;
    }
    
    @Override
    public String usage() {
      return getName() + " <group>=<col fam>{,<col fam>}{ <group>=<col fam>{,<col fam>}}";
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      
      tableOpt = new Option(tableOption, "table", true, "get locality groups for specified table");
      tableOpt.setArgName("table");
      tableOpt.setRequired(true);
      opts.addOption(tableOpt);
      return opts;
    }
    
  }
  
  public static class ImportDirectoryCommand extends Command {
    private static final String DEFAULT_FILE_THREADS = "8";
    private static final String DEFAULT_ASSIGN_THREADS = "20";
    
    private Option numFileThreadsOpt, numAssignThreadsOpt, disableGCOpt, verboseOption;
    
    @Override
    public String description() {
      return "bulk imports an entire directory of data files to the current table";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException, AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      shellState.checkTableState();
      
      String dir = cl.getArgs()[0];
      String failureDir = cl.getArgs()[1];
      int numFileThreads = Integer.parseInt(cl.getOptionValue(numFileThreadsOpt.getOpt(), DEFAULT_FILE_THREADS));
      int numAssignThreads = Integer.parseInt(cl.getOptionValue(numAssignThreadsOpt.getOpt(), DEFAULT_ASSIGN_THREADS));
      boolean disableGC = cl.hasOption(disableGCOpt.getOpt());
      
      AssignmentStats stats = shellState.connector.tableOperations().importDirectory(shellState.tableName, dir, failureDir, numFileThreads, numAssignThreads,
          disableGC);
      if (cl.hasOption(verboseOption.getOpt()))
        shellState.reader.printString(stats.toString());
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 2;
    }
    
    @Override
    public String usage() {
      return getName() + " <directory> <failureDirectory>";
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      
      verboseOption = new Option("v", "verbose", false, "displays statistics from the import");
      opts.addOption(verboseOption);
      
      numFileThreadsOpt = new Option("f", "numFileThreads", true, "number of threads to process files (default: " + DEFAULT_FILE_THREADS + ")");
      numFileThreadsOpt.setArgName("num");
      opts.addOption(numFileThreadsOpt);
      
      numAssignThreadsOpt = new Option("a", "numAssignThreads", true, "number of assign threads for import (default: " + DEFAULT_ASSIGN_THREADS + ")");
      numAssignThreadsOpt.setArgName("num");
      opts.addOption(numAssignThreadsOpt);
      
      disableGCOpt = new Option("g", "disableGC", false, "prevents imported files from being deleted by the garbage collector");
      opts.addOption(disableGCOpt);
      
      return opts;
    }
  }
  
  public interface PrintLine {
    public void print(String s);
    
    public void close();
  }
  
  public static class PrintShell implements PrintLine {
    ConsoleReader reader;
    
    public PrintShell(ConsoleReader reader) {
      this.reader = reader;
    }
    
    public void print(String s) {
      try {
        reader.printString(s + "\n");
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    public void close() {}
  };
  
  public static class PrintFile implements PrintLine {
    PrintWriter writer;
    
    public PrintFile(String filename) throws FileNotFoundException {
      writer = new PrintWriter(filename);
    }
    
    public void print(String s) {
      writer.println(s);
    }
    
    public void close() {
      writer.close();
    }
  };
  
  public static class GetSplitsCommand extends Command {
    
    private Option outputFileOpt, maxSplitsOpt, base64Opt, verboseOpt;
    
    @Override
    public String description() {
      return "retrieves the current split points for tablets in the current table";
    }
    
    private static String encode(boolean encode, Text text) {
      if (text == null)
        return null;
      return encode ? new String(Base64.encodeBase64(TextUtil.getBytes(text))) : text.toString();
    }
    
    private static String obscuredTabletName(KeyExtent extent) {
      MessageDigest digester;
      try {
        digester = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
      if (extent.getEndRow() != null && extent.getEndRow().getLength() > 0) {
        digester.update(extent.getEndRow().getBytes(), 0, extent.getEndRow().getLength());
      }
      return new String(Base64.encodeBase64(digester.digest()));
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, final Shell shellState) throws IOException, AccumuloException, AccumuloSecurityException,
        TableNotFoundException {
      shellState.checkTableState();
      final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
      final String m = cl.getOptionValue(maxSplitsOpt.getOpt());
      final int maxSplits = m == null ? 0 : Integer.parseInt(m);
      final boolean encode = cl.hasOption(base64Opt.getOpt());
      final boolean verbose = cl.hasOption(verboseOpt.getOpt());
      
      PrintLine p = outputFile == null ? new PrintShell(shellState.reader) : new PrintFile(outputFile);
      
      try {
        if (!verbose) {
          for (Text row : maxSplits > 0 ? shellState.connector.tableOperations().getSplits(shellState.tableName, maxSplits) : shellState.connector
              .tableOperations().getSplits(shellState.tableName)) {
            p.print(encode(encode, row));
          }
        } else {
          final Scanner scanner = shellState.connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
          ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
          final Text start = new Text(shellState.connector.tableOperations().tableIdMap().get(shellState.tableName));
          final Text end = new Text(start);
          end.append(new byte[] {'<'}, 0, 1);
          scanner.setRange(new Range(start, end));
          for (Iterator<Entry<Key,Value>> iterator = scanner.iterator(); iterator.hasNext();) {
            final Entry<Key,Value> next = iterator.next();
            if (Constants.METADATA_PREV_ROW_COLUMN.hasColumns(next.getKey())) {
              KeyExtent extent = new KeyExtent(next.getKey().getRow(), next.getValue());
              final String pr = encode(encode, extent.getPrevEndRow());
              final String er = encode(encode, extent.getEndRow());
              final String line = String.format("%-26s (%s, %s%s", obscuredTabletName(extent), pr == null ? "-inf" : pr, er == null ? "+inf" : er,
                  er == null ? ") Default Tablet " : "]");
              p.print(line);
            }
          }
        }
        
      } finally {
        p.close();
      }
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      
      outputFileOpt = new Option("o", "output", true, "specifies a local file to write the splits to");
      outputFileOpt.setArgName("file");
      opts.addOption(outputFileOpt);
      
      maxSplitsOpt = new Option("m", "max", true, "specifies the maximum number of splits to create");
      maxSplitsOpt.setArgName("num");
      opts.addOption(maxSplitsOpt);
      
      base64Opt = new Option("b64", "base64encoded", false, "encode the split points");
      opts.addOption(base64Opt);
      
      verboseOpt = new Option("v", "verbose", false, "print out the tablet information with start/end rows");
      opts.addOption(verboseOpt);
      
      return opts;
    }
  }
  
  private static class ActiveScanIterator implements Iterator<String> {
    
    private InstanceOperations instanceOps;
    private Iterator<String> tsIter;
    private Iterator<String> scansIter;
    
    private void readNext() {
      List<String> scans = new ArrayList<String>();
      
      while (tsIter.hasNext()) {
        
        String tserver = tsIter.next();
        try {
          List<ActiveScan> asl = instanceOps.getActiveScans(tserver);
          
          for (ActiveScan as : asl)
            scans.add(String.format("%21s |%21s |%9s |%9s |%7s |%6s |%8s |%8s |%10s |%10s |%10s | %s", tserver, as.getClient(),
                Duration.format(as.getAge(), ""), Duration.format(as.getLastContactTime(), ""), as.getState(), as.getType(), as.getUser(), as.getTable(),
                as.getColumns(), (as.getType() == ScanType.SINGLE ? as.getExtent() : "N/A"), as.getSsiList(), as.getSsio()));
          
        } catch (Exception e) {
          scans.add(tserver + " ERROR " + e.getMessage());
        }
        
        if (scans.size() > 0)
          break;
      }
      
      scansIter = scans.iterator();
    }
    
    ActiveScanIterator(List<String> tservers, InstanceOperations instanceOps) {
      this.instanceOps = instanceOps;
      this.tsIter = tservers.iterator();
      
      String header = String.format(" %-21s| %-21s| %-9s| %-9s| %-7s| %-6s| %-8s| %-8s| %-10s| %-10s| %-10s | %s", "TABLET SERVER", "CLIENT", "AGE", "LAST",
          "STATE", "TYPE", "USER", "TABLE", "COLUMNS", "TABLET", "ITERATORS", "ITERATOR OPTIONS");
      
      scansIter = Collections.singletonList(header).iterator();
    }
    
    @Override
    public boolean hasNext() {
      return scansIter.hasNext();
    }
    
    @Override
    public String next() {
      String next = scansIter.next();
      
      if (!scansIter.hasNext())
        readNext();
      
      return next;
    }
    
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
    
  }
  
  public static class ListScansCommand extends Command {
    
    private Option tserverOption, disablePaginationOpt;
    
    @Override
    public String description() {
      return "list what scans are currently running in accumulo. See the accumulo.core.client.admin.ActiveScan javadoc for more information about columns.";
    }
    
    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      
      List<String> tservers;
      
      InstanceOperations instanceOps = shellState.connector.instanceOperations();
      
      boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());
      
      if (cl.hasOption(tserverOption.getOpt())) {
        tservers = new ArrayList<String>();
        tservers.add(cl.getOptionValue(tserverOption.getOpt()));
      } else {
        tservers = instanceOps.getTabletServers();
      }
      
      shellState.printLines(new ActiveScanIterator(tservers, instanceOps), paginate);
      
      return 0;
    }
    
    @Override
    public int numArgs() {
      return 0;
    }
    
    @Override
    public Options getOptions() {
      Options opts = new Options();
      
      tserverOption = new Option("ts", "tabletServer", true, "list scans for a specific tablet server");
      tserverOption.setArgName("tablet server");
      opts.addOption(tserverOption);
      
      disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
      opts.addOption(disablePaginationOpt);
      
      return opts;
    }
    
  }
  
  private final void printLines(Iterator<String> lines, boolean paginate) throws IOException {
    int linesPrinted = 0;
    String prompt = "-- hit any key to continue or 'q' to quit --";
    int lastPromptLength = prompt.length();
    int termWidth = reader.getTermwidth();
    int maxLines = reader.getTermheight();
    
    String peek = null;
    while (lines.hasNext()) {
      String nextLine = lines.next();
      if (nextLine == null)
        continue;
      for (String line : nextLine.split("\\n")) {
        if (peek != null) {
          reader.printString(peek);
          reader.printNewline();
          if (paginate) {
            linesPrinted += peek.length() == 0 ? 0 : Math.ceil(peek.length() * 1.0 / termWidth);
            
            // check if displaying the next line would result in
            // scrolling off the screen
            if (linesPrinted + Math.ceil(lastPromptLength * 1.0 / termWidth) + Math.ceil(prompt.length() * 1.0 / termWidth)
                + Math.ceil(line.length() * 1.0 / termWidth) > maxLines) {
              linesPrinted = 0;
              int numdashes = (termWidth - prompt.length()) / 2;
              String nextPrompt = repeat("-", numdashes) + prompt + repeat("-", numdashes);
              lastPromptLength = nextPrompt.length();
              reader.printString(nextPrompt);
              reader.flushConsole();
              if (Character.toUpperCase((char) reader.readVirtualKey()) == 'Q') {
                reader.printNewline();
                return;
              }
              reader.printNewline();
              termWidth = reader.getTermwidth();
              maxLines = reader.getTermheight();
            }
          }
        }
        peek = line;
      }
    }
    if (peek != null) {
      reader.printString(peek);
      reader.printNewline();
    }
  }
  
  private final void printRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate) throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, printTimestamps), paginate);
  }
  
  private static String repeat(String s, int c) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < c; i++)
      sb.append(s);
    return sb.toString();
  }
  
  private static Authorizations parseAuthorizations(String field) {
    if (field == null || field.isEmpty())
      return Constants.NO_AUTHS;
    return new Authorizations(field.split(","));
  }
  
  private void checkTableState() {
    if (tableName.isEmpty())
      throw new IllegalStateException("Not in a table context. Please use 'table <tableName>' to switch to a table");
  }
  
  private final void printConstraintViolationException(ConstraintViolationException cve) {
    printException(cve, "");
    int COL1 = 50, COL2 = 14;
    int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTermwidth() - COL1 - COL2 - 6));
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    log.error(String.format("%-" + COL1 + "s | %" + COL2 + "s | %-" + col3 + "s\n", "Constraint class", "Violation code", "Violation Description"));
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    for (TConstraintViolationSummary cvs : cve.violationSummaries)
      log.error(String.format("%-" + COL1 + "s | %" + COL2 + "d | %-" + col3 + "s\n", cvs.constrainClass, cvs.violationCode, cvs.violationDescription));
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
  }
  
  private static final void printException(Exception e) {
    printException(e, e.getMessage());
  }
  
  private static final void printException(Exception e, String msg) {
    log.error(e.getClass().getName() + (msg != null ? ": " + msg : ""));
    log.debug(e.getClass().getName() + (msg != null ? ": " + msg : ""), e);
  }
  
  private static final void setDebugging(boolean debuggingEnabled) {
    Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(debuggingEnabled ? Level.TRACE : Level.INFO);
  }
  
  public static final boolean isDebuggingEnabled() {
    return Logger.getLogger(Constants.CORE_PACKAGE_NAME).isTraceEnabled();
  }
  
  private static final void printHelp(String usage, String description, Options opts) {
    PrintWriter pw = new PrintWriter(System.err);
    new HelpFormatter().printHelp(pw, Integer.MAX_VALUE, usage, description, opts, 2, 5, null, true);
    pw.flush();
  }
}
