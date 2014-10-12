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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jline.ConsoleReader;
import jline.History;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.security.AuditLevel;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.format.BinaryFormatter;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterFactory;
import org.apache.accumulo.core.util.shell.commands.AboutCommand;
import org.apache.accumulo.core.util.shell.commands.AddAuthsCommand;
import org.apache.accumulo.core.util.shell.commands.AddSplitsCommand;
import org.apache.accumulo.core.util.shell.commands.AuthenticateCommand;
import org.apache.accumulo.core.util.shell.commands.ByeCommand;
import org.apache.accumulo.core.util.shell.commands.ClasspathCommand;
import org.apache.accumulo.core.util.shell.commands.ClearCommand;
import org.apache.accumulo.core.util.shell.commands.CloneTableCommand;
import org.apache.accumulo.core.util.shell.commands.ClsCommand;
import org.apache.accumulo.core.util.shell.commands.CompactCommand;
import org.apache.accumulo.core.util.shell.commands.ConfigCommand;
import org.apache.accumulo.core.util.shell.commands.ConstraintCommand;
import org.apache.accumulo.core.util.shell.commands.CreateTableCommand;
import org.apache.accumulo.core.util.shell.commands.CreateUserCommand;
import org.apache.accumulo.core.util.shell.commands.DUCommand;
import org.apache.accumulo.core.util.shell.commands.DebugCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteIterCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteManyCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteRowsCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteScanIterCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteShellIterCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteTableCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteUserCommand;
import org.apache.accumulo.core.util.shell.commands.DropTableCommand;
import org.apache.accumulo.core.util.shell.commands.DropUserCommand;
import org.apache.accumulo.core.util.shell.commands.EGrepCommand;
import org.apache.accumulo.core.util.shell.commands.ExecfileCommand;
import org.apache.accumulo.core.util.shell.commands.ExitCommand;
import org.apache.accumulo.core.util.shell.commands.ExportTableCommand;
import org.apache.accumulo.core.util.shell.commands.FlushCommand;
import org.apache.accumulo.core.util.shell.commands.FormatterCommand;
import org.apache.accumulo.core.util.shell.commands.GetAuthsCommand;
import org.apache.accumulo.core.util.shell.commands.GetGroupsCommand;
import org.apache.accumulo.core.util.shell.commands.GetSplitsCommand;
import org.apache.accumulo.core.util.shell.commands.GrantCommand;
import org.apache.accumulo.core.util.shell.commands.GrepCommand;
import org.apache.accumulo.core.util.shell.commands.HelpCommand;
import org.apache.accumulo.core.util.shell.commands.HiddenCommand;
import org.apache.accumulo.core.util.shell.commands.HistoryCommand;
import org.apache.accumulo.core.util.shell.commands.ImportDirectoryCommand;
import org.apache.accumulo.core.util.shell.commands.ImportTableCommand;
import org.apache.accumulo.core.util.shell.commands.InfoCommand;
import org.apache.accumulo.core.util.shell.commands.InsertCommand;
import org.apache.accumulo.core.util.shell.commands.InterpreterCommand;
import org.apache.accumulo.core.util.shell.commands.ListCompactionsCommand;
import org.apache.accumulo.core.util.shell.commands.ListIterCommand;
import org.apache.accumulo.core.util.shell.commands.ListScansCommand;
import org.apache.accumulo.core.util.shell.commands.ListShellIterCommand;
import org.apache.accumulo.core.util.shell.commands.MaxRowCommand;
import org.apache.accumulo.core.util.shell.commands.MergeCommand;
import org.apache.accumulo.core.util.shell.commands.NoTableCommand;
import org.apache.accumulo.core.util.shell.commands.OfflineCommand;
import org.apache.accumulo.core.util.shell.commands.OnlineCommand;
import org.apache.accumulo.core.util.shell.commands.PasswdCommand;
import org.apache.accumulo.core.util.shell.commands.PingCommand;
import org.apache.accumulo.core.util.shell.commands.QuestionCommand;
import org.apache.accumulo.core.util.shell.commands.QuitCommand;
import org.apache.accumulo.core.util.shell.commands.QuotedStringTokenizer;
import org.apache.accumulo.core.util.shell.commands.RenameTableCommand;
import org.apache.accumulo.core.util.shell.commands.RevokeCommand;
import org.apache.accumulo.core.util.shell.commands.ScanCommand;
import org.apache.accumulo.core.util.shell.commands.SetAuthsCommand;
import org.apache.accumulo.core.util.shell.commands.SetGroupsCommand;
import org.apache.accumulo.core.util.shell.commands.SetIterCommand;
import org.apache.accumulo.core.util.shell.commands.SetScanIterCommand;
import org.apache.accumulo.core.util.shell.commands.SetShellIterCommand;
import org.apache.accumulo.core.util.shell.commands.SleepCommand;
import org.apache.accumulo.core.util.shell.commands.SystemPermissionsCommand;
import org.apache.accumulo.core.util.shell.commands.TableCommand;
import org.apache.accumulo.core.util.shell.commands.TablePermissionsCommand;
import org.apache.accumulo.core.util.shell.commands.TablesCommand;
import org.apache.accumulo.core.util.shell.commands.TraceCommand;
import org.apache.accumulo.core.util.shell.commands.UserCommand;
import org.apache.accumulo.core.util.shell.commands.UserPermissionsCommand;
import org.apache.accumulo.core.util.shell.commands.UsersCommand;
import org.apache.accumulo.core.util.shell.commands.WhoAmICommand;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A convenient console interface to perform basic accumulo functions Includes auto-complete, help, and quoted strings with escape sequences
 */
public class Shell extends ShellOptions {
  public static final Logger log = Logger.getLogger(Shell.class);
  private static final Logger audit = Logger.getLogger(Shell.class.getName() + ".audit");
  
  public static final String CHARSET = "ISO-8859-1";
  public static final int NO_FIXED_ARG_LENGTH_CHECK = -1;
  private static final String SHELL_DESCRIPTION = "Shell - Apache Accumulo Interactive Shell";
  private static final String DEFAULT_AUTH_TIMEOUT = "60"; // in minutes
  
  protected int exitCode = 0;
  private String tableName;
  protected Instance instance;
  private Connector connector;
  protected ConsoleReader reader;
  private String principal;
  private AuthenticationToken token;
  private final Class<? extends Formatter> defaultFormatterClass = DefaultFormatter.class;
  private final Class<? extends Formatter> binaryFormatterClass = BinaryFormatter.class;
  public Map<String,List<IteratorSetting>> scanIteratorOptions = new HashMap<String,List<IteratorSetting>>();
  public Map<String,List<IteratorSetting>> iteratorProfiles = new HashMap<String,List<IteratorSetting>>();
  
  private Token rootToken;
  public final Map<String,Command> commandFactory = new TreeMap<String,Command>();
  public final Map<String,Command[]> commandGrouping = new TreeMap<String,Command[]>();
  protected boolean configError = false;
  
  // exit if true
  private boolean exit = false;
  
  // file to execute commands from
  protected String execFile = null;
  // single command to execute from the command line
  protected String execCommand = null;
  protected boolean verbose = true;
  
  private boolean tabCompletion;
  private boolean disableAuthTimeout;
  private long authTimeout;
  private long lastUserActivity = System.nanoTime();
  private boolean logErrorsToConsole = false;
  private PrintWriter writer = null;
  private boolean masking = false;
  
  public Shell() throws IOException {
    this(new ConsoleReader(), new PrintWriter(
        new OutputStreamWriter(System.out,
        System.getProperty("jline.WindowsTerminal.output.encoding", System.getProperty("file.encoding")))));
  }
  
  public Shell(ConsoleReader reader, PrintWriter writer) {
    super();
    this.reader = reader;
    this.writer = writer;
  }
  
  // Not for client use
  public boolean config(String... args) {
    
    CommandLine cl;
    try {
      cl = new BasicParser().parse(opts, args);
      if (cl.getArgs().length > 0)
        throw new ParseException("Unrecognized arguments: " + cl.getArgList());
      
      if (cl.hasOption(helpOpt.getOpt())) {
        configError = true;
        printHelp("shell", SHELL_DESCRIPTION, opts);
        return true;
      }
      
      setDebugging(cl.hasOption(debugOption.getLongOpt()));
      authTimeout = TimeUnit.MINUTES.toNanos(Integer.parseInt(cl.getOptionValue(authTimeoutOpt.getLongOpt(), DEFAULT_AUTH_TIMEOUT)));
      disableAuthTimeout = cl.hasOption(disableAuthTimeoutOpt.getLongOpt());
      
      if (cl.hasOption(zooKeeperInstance.getOpt()) && cl.getOptionValues(zooKeeperInstance.getOpt()).length != 2)
        throw new MissingArgumentException(zooKeeperInstance);
      
    } catch (Exception e) {
      configError = true;
      printException(e);
      printHelp("shell", SHELL_DESCRIPTION, opts);
      return true;
    }
    
    // get the options that were parsed
    String sysUser = System.getProperty("user.name");
    if (sysUser == null)
      sysUser = "root";
    String user = cl.getOptionValue(usernameOption.getOpt(), sysUser);
    
    String passw = cl.getOptionValue(passwOption.getOpt(), null);
    tabCompletion = !cl.hasOption(tabCompleteOption.getLongOpt());
    String[] loginOptions = cl.getOptionValues(loginOption.getOpt());
    
    // Use a fake (Mock), ZK, or HdfsZK Accumulo instance
    setInstance(cl);
    
    // process default parameters if unspecified
    try {
      if (loginOptions != null && !cl.hasOption(tokenOption.getOpt()))
        throw new IllegalArgumentException("Must supply '-" + tokenOption.getOpt() + "' option with '-" + loginOption.getOpt() + "' option");
      
      if (loginOptions == null && cl.hasOption(tokenOption.getOpt()))
        throw new IllegalArgumentException("Must supply '-" + loginOption.getOpt() + "' option with '-" + tokenOption.getOpt() + "' option");
      
      if (passw != null && cl.hasOption(tokenOption.getOpt()))
        throw new IllegalArgumentException("Can not supply '-" + passwOption.getOpt() + "' option with '-" + tokenOption.getOpt() + "' option");
      
      if (user == null)
        throw new MissingArgumentException(usernameOption);
      
      if (loginOptions != null && cl.hasOption(tokenOption.getOpt())) {
        Properties props = new Properties();
        for (String loginOption : loginOptions)
          for (String lo : loginOption.split(",")) {
            String[] split = lo.split("=");
            props.put(split[0], split[1]);
          }
        
        this.token = Class.forName(cl.getOptionValue(tokenOption.getOpt())).asSubclass(AuthenticationToken.class).newInstance();
        this.token.init(props);
      }
      
      if (!cl.hasOption(fakeOption.getLongOpt())) {
        DistributedTrace.enable(instance, new ZooReader(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut()), "shell", InetAddress.getLocalHost()
            .getHostName());
      }
      
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void start() {
          reader.getTerminal().enableEcho();
        }
      });
      
      if (passw != null) {
        this.token = new PasswordToken(passw);
      }
      
      if (this.token == null) {
        passw = readMaskedLine("Password: ", '*');
        if (passw != null)
          this.token = new PasswordToken(passw);
      }
      
      if (this.token == null) {
        reader.printNewline();
        throw new MissingArgumentException("No password or token option supplied");
      } // user canceled
      
      this.setTableName("");
      this.principal = user;
      connector = instance.getConnector(this.principal, token);
      
    } catch (Exception e) {
      printException(e);
      configError = true;
    }
    
    // decide whether to execute commands from a file and quit
    if (cl.hasOption(execfileOption.getOpt())) {
      execFile = cl.getOptionValue(execfileOption.getOpt());
      verbose = false;
    } else if (cl.hasOption(execfileVerboseOption.getOpt())) {
      execFile = cl.getOptionValue(execfileVerboseOption.getOpt());
    }
    if (cl.hasOption(execCommandOpt.getOpt())) {
      execCommand = cl.getOptionValue(execCommandOpt.getOpt());
      verbose = false;
    }
    
    rootToken = new Token();
    
    Command[] dataCommands = {new DeleteCommand(), new DeleteManyCommand(), new DeleteRowsCommand(), new EGrepCommand(), new FormatterCommand(),
        new InterpreterCommand(), new GrepCommand(), new ImportDirectoryCommand(), new InsertCommand(), new MaxRowCommand(), new ScanCommand()};
    Command[] debuggingCommands = {new ClasspathCommand(), new DebugCommand(), new ListScansCommand(), new ListCompactionsCommand(), new TraceCommand(),
        new PingCommand()};
    Command[] execCommands = {new ExecfileCommand(), new HistoryCommand()};
    Command[] exitCommands = {new ByeCommand(), new ExitCommand(), new QuitCommand()};
    Command[] helpCommands = {new AboutCommand(), new HelpCommand(), new InfoCommand(), new QuestionCommand()};
    Command[] iteratorCommands = {new DeleteIterCommand(), new DeleteScanIterCommand(), new ListIterCommand(), new SetIterCommand(), new SetScanIterCommand(),
        new SetShellIterCommand(), new ListShellIterCommand(), new DeleteShellIterCommand()};
    Command[] otherCommands = {new HiddenCommand()};
    Command[] permissionsCommands = {new GrantCommand(), new RevokeCommand(), new SystemPermissionsCommand(), new TablePermissionsCommand(),
        new UserPermissionsCommand()};
    Command[] stateCommands = {new AuthenticateCommand(), new ClsCommand(), new ClearCommand(), new NoTableCommand(), new SleepCommand(), new TableCommand(),
        new UserCommand(), new WhoAmICommand()};
    Command[] tableCommands = {new CloneTableCommand(), new ConfigCommand(), new CreateTableCommand(), new DeleteTableCommand(), new DropTableCommand(),
        new DUCommand(), new ExportTableCommand(), new ImportTableCommand(), new OfflineCommand(), new OnlineCommand(), new RenameTableCommand(),
        new TablesCommand()};
    Command[] tableControlCommands = {new AddSplitsCommand(), new CompactCommand(), new ConstraintCommand(), new FlushCommand(), new GetGroupsCommand(),
        new GetSplitsCommand(), new MergeCommand(), new SetGroupsCommand()};
    Command[] userCommands = {new AddAuthsCommand(), new CreateUserCommand(), new DeleteUserCommand(), new DropUserCommand(), new GetAuthsCommand(),
        new PasswdCommand(), new SetAuthsCommand(), new UsersCommand()};
    commandGrouping.put("-- Writing, Reading, and Removing Data --", dataCommands);
    commandGrouping.put("-- Debugging Commands -------------------", debuggingCommands);
    commandGrouping.put("-- Shell Execution Commands -------------", execCommands);
    commandGrouping.put("-- Exiting Commands ---------------------", exitCommands);
    commandGrouping.put("-- Help Commands ------------------------", helpCommands);
    commandGrouping.put("-- Iterator Configuration ---------------", iteratorCommands);
    commandGrouping.put("-- Permissions Administration Commands --", permissionsCommands);
    commandGrouping.put("-- Shell State Commands -----------------", stateCommands);
    commandGrouping.put("-- Table Administration Commands --------", tableCommands);
    commandGrouping.put("-- Table Control Commands ---------------", tableControlCommands);
    commandGrouping.put("-- User Administration Commands ---------", userCommands);
    
    for (Command[] cmds : commandGrouping.values()) {
      for (Command cmd : cmds)
        commandFactory.put(cmd.getName(), cmd);
    }
    for (Command cmd : otherCommands) {
      commandFactory.put(cmd.getName(), cmd);
    }
    return configError;
  }
  
  protected void setInstance(CommandLine cl) {
    // should only be one instance option set
    instance = null;
    if (cl.hasOption(fakeOption.getLongOpt())) {
      instance = new MockInstance("fake");
    } else if (cl.hasOption(hdfsZooInstance.getOpt())) {
      @SuppressWarnings("deprecation")
      AccumuloConfiguration deprecatedSiteConfiguration = AccumuloConfiguration.getSiteConfiguration();
      instance = getDefaultInstance(deprecatedSiteConfiguration);
    } else if (cl.hasOption(zooKeeperInstance.getOpt())) {
      String[] zkOpts = cl.getOptionValues(zooKeeperInstance.getOpt());
      instance = new ZooKeeperInstance(zkOpts[0], zkOpts[1]);
    } else {
      @SuppressWarnings("deprecation")
      AccumuloConfiguration deprecatedSiteConfiguration = AccumuloConfiguration.getSiteConfiguration();
      instance = getDefaultInstance(deprecatedSiteConfiguration);
    }
  }
  
  @SuppressWarnings("deprecation")
  private static Instance getDefaultInstance(AccumuloConfiguration conf) {
    String keepers = conf.get(Property.INSTANCE_ZK_HOST);
    Path instanceDir = new Path(conf.get(Property.INSTANCE_DFS_DIR), "instance_id");
    return new ZooKeeperInstance(UUID.fromString(ZooKeeperInstance.getInstanceIDFromHdfs(instanceDir)), keepers);
  }
  
  public Connector getConnector() {
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
    if (isVerbose())
      printInfo();
    
    String home = System.getProperty("HOME");
    if (home == null)
      home = System.getenv("HOME");
    String configDir = home + "/.accumulo";
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
    
    ShellCompletor userCompletor = null;
    
    if (execFile != null) {
      java.util.Scanner scanner = new java.util.Scanner(new File(execFile), Constants.UTF8.name());
      try {
        while (scanner.hasNextLine() && !hasExited()) {
          execCommand(scanner.nextLine(), true, isVerbose());
        }
      } finally {
        scanner.close();
      }
    } else if (execCommand != null) {
      for (String command : execCommand.split("\n")) {
        execCommand(command, true, isVerbose());
      }
      return exitCode;
    }
    
    while (true) {
      if (hasExited())
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
  
  public void printInfo() throws IOException {
    reader.printString("\n" + SHELL_DESCRIPTION + "\n" + "- \n" + "- version: " + Constants.VERSION + "\n" + "- instance name: "
        + connector.getInstance().getInstanceName() + "\n" + "- instance id: " + connector.getInstance().getInstanceID() + "\n" + "- \n"
        + "- type 'help' for a list of available commands\n" + "- \n");
    reader.flushConsole();
  }
  
  public void printVerboseInfo() throws IOException {
    StringBuilder sb = new StringBuilder("-\n");
    sb.append("- Current user: ").append(connector.whoami()).append("\n");
    if (execFile != null)
      sb.append("- Executing commands from: ").append(execFile).append("\n");
    if (disableAuthTimeout)
      sb.append("- Authorization timeout: disabled\n");
    else
      sb.append("- Authorization timeout: ").append(String.format("%ds%n", TimeUnit.NANOSECONDS.toSeconds(authTimeout)));
    sb.append("- Debug: ").append(isDebuggingEnabled() ? "on" : "off").append("\n");
    if (!scanIteratorOptions.isEmpty()) {
      for (Entry<String,List<IteratorSetting>> entry : scanIteratorOptions.entrySet()) {
        sb.append("- Session scan iterators for table ").append(entry.getKey()).append(":\n");
        for (IteratorSetting setting : entry.getValue()) {
          sb.append("-    Iterator ").append(setting.getName()).append(" options:\n");
          sb.append("-        ").append("iteratorPriority").append(" = ").append(setting.getPriority()).append("\n");
          sb.append("-        ").append("iteratorClassName").append(" = ").append(setting.getIteratorClass()).append("\n");
          for (Entry<String,String> optEntry : setting.getOptions().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-\n");
    reader.printString(sb.toString());
  }
  
  public String getDefaultPrompt() {
    return connector.whoami() + "@" + connector.getInstance().getInstanceName() + (getTableName().isEmpty() ? "" : " ") + getTableName() + "> ";
  }
  
  public void execCommand(String input, boolean ignoreAuthTimeout, boolean echoPrompt) throws IOException {
    audit.log(AuditLevel.AUDIT, getDefaultPrompt() + input);
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
        sc = commandFactory.get(command);
        if (sc == null) {
          reader.printString(String.format("Unknown command \"%s\".  Enter \"help\" for a list possible commands.%n", command));
          reader.flushConsole();
          return;
        }
        
        long duration = System.nanoTime() - lastUserActivity;
        if (!(sc instanceof ExitCommand) && !ignoreAuthTimeout && (duration < 0 || duration > authTimeout)) {
          reader.printString("Shell has been idle for too long. Please re-authenticate.\n");
          boolean authFailed = true;
          do {
            String pwd = readMaskedLine("Enter current password for '" + connector.whoami() + "': ", '*');
            if (pwd == null) {
              reader.printNewline();
              return;
            } // user canceled
            
            try {
              authFailed = !connector.securityOperations().authenticateUser(connector.whoami(), new PasswordToken(pwd));
            } catch (Exception e) {
              ++exitCode;
              printException(e);
            }
            
            if (authFailed)
              reader.printString("Invalid password. ");
          } while (authFailed);
          lastUserActivity = System.nanoTime();
        }
        
        // Get the options from the command on how to parse the string
        Options parseOpts = sc.getOptionsWithHelp();
        
        // Parse the string using the given options
        CommandLine cl = new BasicParser().parse(parseOpts, fields);
        
        int actualArgLen = cl.getArgs().length;
        int expectedArgLen = sc.numArgs();
        if (cl.hasOption(helpOption)) {
          // Display help if asked to; otherwise execute the command
          sc.printHelp(this);
        } else if (expectedArgLen != NO_FIXED_ARG_LENGTH_CHECK && actualArgLen != expectedArgLen) {
          ++exitCode;
          // Check for valid number of fixed arguments (if not
          // negative; negative means it is not checked, for
          // vararg-like commands)
          printException(new IllegalArgumentException(String.format("Expected %d argument%s. There %s %d.", expectedArgLen, expectedArgLen == 1 ? "" : "s",
              actualArgLen == 1 ? "was" : "were", actualArgLen)));
          sc.printHelp(this);
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
        if (getTableName().equals(e.getTableName()))
          setTableName("");
        printException(e);
      } catch (ParseException e) {
        // not really an error if the exception is a missing required
        // option when the user is asking for help
        if (!(e instanceof MissingOptionException && (Arrays.asList(fields).contains("-" + helpOption) || Arrays.asList(fields).contains("--" + helpLongOption)))) {
          ++exitCode;
          printException(e);
        }
        if (sc != null)
          sc.printHelp(this);
      } catch (Exception e) {
        ++exitCode;
        printException(e);
      }
    } else {
      ++exitCode;
      printException(new BadArgumentException("Unrecognized empty command", command, -1));
    }
    reader.flushConsole();
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
      userlist = connector.securityOperations().listLocalUsers();
    } catch (Exception e) {
      log.debug("Unable to obtain list of users", e);
      userlist = Collections.emptySet();
    }
    
    Map<Command.CompletionSet,Set<String>> options = new HashMap<Command.CompletionSet,Set<String>>();
    
    Set<String> commands = new HashSet<String>();
    for (String a : commandFactory.keySet())
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
    
    for (Command[] cmdGroup : commandGrouping.values()) {
      for (Command c : cmdGroup) {
        c.getOptionsWithHelp(); // prep the options for the command
        // so that the completion can
        // include them
        c.registerCompletion(rootToken, options);
      }
    }
    return new ShellCompletor(rootToken, options);
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
    
    /**
     * If the number of arguments is not always zero (not including those arguments handled through Options), make sure to override the {@link #usage()} method.
     * Otherwise, {@link #usage()} does need to be overridden.
     */
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
    public final void printHelp(Shell shellState) {
      shellState.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp());
    }
    
    public final void printHelp(Shell shellState, int width) {
      shellState.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp(), width);
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
  
  public interface PrintLine {
    public void print(String s);
    
    public void close();
  }
  
  public static class PrintShell implements PrintLine {
    ConsoleReader reader;
    
    public PrintShell(ConsoleReader reader) {
      this.reader = reader;
    }
    
    @Override
    public void print(String s) {
      try {
        reader.printString(s + "\n");
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    
    @Override
    public void close() {}
  };
  
  public static class PrintFile implements PrintLine {
    PrintWriter writer;
    
    public PrintFile(String filename) throws FileNotFoundException {
      writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), Constants.UTF8)));
    }
    
    @Override
    public void print(String s) {
      writer.println(s);
    }
    
    @Override
    public void close() {
      writer.close();
    }
  };
  
  public final void printLines(Iterator<String> lines, boolean paginate) throws IOException {
    printLines(lines, paginate, null);
  }
  
  public final void printLines(Iterator<String> lines, boolean paginate, PrintLine out) throws IOException {
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
        if (out == null) {
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
        } else {
          out.print(line);
        }
      }
    }
    if (out == null && peek != null) {
      reader.printString(peek);
      reader.printNewline();
    }
  }
  
  public final void printRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate, Class<? extends Formatter> formatterClass,
      PrintLine outFile) throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, printTimestamps), paginate, outFile);
  }
  
  public final void printRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate, Class<? extends Formatter> formatterClass)
      throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, printTimestamps), paginate);
  }
  
  public final void printBinaryRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate, PrintLine outFile) throws IOException {
    printLines(FormatterFactory.getFormatter(binaryFormatterClass, scanner, printTimestamps), paginate, outFile);
  }
  
  public final void printBinaryRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate) throws IOException {
    printLines(FormatterFactory.getFormatter(binaryFormatterClass, scanner, printTimestamps), paginate);
  }
  
  public static String repeat(String s, int c) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < c; i++)
      sb.append(s);
    return sb.toString();
  }
  
  public void checkTableState() {
    if (getTableName().isEmpty())
      throw new IllegalStateException(
          "Not in a table context. Please use 'table <tableName>' to switch to a table, or use '-t' to specify a table if option is available.");
  }
  
  private final void printConstraintViolationException(ConstraintViolationException cve) {
    printException(cve, "");
    int COL1 = 50, COL2 = 14;
    int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTermwidth() - COL1 - COL2 - 6));
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    logError(String.format("%-" + COL1 + "s | %" + COL2 + "s | %-" + col3 + "s%n", "Constraint class", "Violation code", "Violation Description"));
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    for (TConstraintViolationSummary cvs : cve.violationSummaries)
      logError(String.format("%-" + COL1 + "s | %" + COL2 + "d | %-" + col3 + "s%n", cvs.constrainClass, cvs.violationCode, cvs.violationDescription));
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
  }
  
  public final void printException(Exception e) {
    printException(e, e.getMessage());
  }
  
  private final void printException(Exception e, String msg) {
    logError(e.getClass().getName() + (msg != null ? ": " + msg : ""));
    log.debug(e.getClass().getName() + (msg != null ? ": " + msg : ""), e);
  }
  
  public static final void setDebugging(boolean debuggingEnabled) {
    Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(debuggingEnabled ? Level.TRACE : Level.INFO);
  }
  
  public static final boolean isDebuggingEnabled() {
    return Logger.getLogger(Constants.CORE_PACKAGE_NAME).isTraceEnabled();
  }
  
  private final void printHelp(String usage, String description, Options opts) {
    printHelp(usage, description, opts, Integer.MAX_VALUE);
  }
  
  private final void printHelp(String usage, String description, Options opts, int width) {
    // TODO Use the OutputStream from the JLine ConsoleReader if we can ever get access to it
    new HelpFormatter().printHelp(writer, width, usage, description, opts, 2, 5, null, true);
    writer.flush();
  }
  
  public int getExitCode() {
    return exitCode;
  }
  
  public void resetExitCode() {
    exitCode = 0;
  }
  
  public void setExit(boolean exit) {
    this.exit = exit;
  }
  
  public boolean getExit() {
    return this.exit;
  }
  
  public boolean isVerbose() {
    return verbose;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public ConsoleReader getReader() {
    return reader;
  }
  
  public void updateUser(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    connector = instance.getConnector(principal, token);
    this.principal = principal;
    this.token = token;
  }
  
  public String getPrincipal() {
    return principal;
  }
  
  public AuthenticationToken getToken() {
    return token;
  }
  
  /**
   * Return the formatter for the current table.
   * 
   * @return the formatter class for the current table
   */
  public Class<? extends Formatter> getFormatter() {
    return getFormatter(this.tableName);
  }
  
  /**
   * Return the formatter for the given table.
   * 
   * @param tableName
   *          the table name
   * @return the formatter class for the given table
   */
  public Class<? extends Formatter> getFormatter(String tableName) {
    Class<? extends Formatter> formatter = FormatterCommand.getCurrentFormatter(tableName, this);
    
    if (null == formatter) {
      logError("Could not load the specified formatter. Using the DefaultFormatter");
      return this.defaultFormatterClass;
    } else {
      return formatter;
    }
  }
  
  public void setLogErrorsToConsole() {
    this.logErrorsToConsole = true;
  }
  
  private void logError(String s) {
    log.error(s);
    if (logErrorsToConsole) {
      try {
        reader.printString("ERROR: " + s + "\n");
        reader.flushConsole();
      } catch (IOException e) {}
    }
  }
  
  public String readMaskedLine(String prompt, Character mask) throws IOException {
    this.masking = true;
    String s = reader.readLine(prompt, mask);
    this.masking = false;
    return s;
  }
  
  public boolean isMasking() {
    return masking;
  }
  
  public boolean hasExited() {
    return exit;
  }
}
