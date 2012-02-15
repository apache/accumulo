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
import java.nio.ByteBuffer;
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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

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
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.security.AuditLevel;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.format.BinaryFormatter;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterFactory;
import org.apache.accumulo.core.util.shell.commands.AboutCommand;
import org.apache.accumulo.core.util.shell.commands.AddSplitsCommand;
import org.apache.accumulo.core.util.shell.commands.AuthenticateCommand;
import org.apache.accumulo.core.util.shell.commands.ByeCommand;
import org.apache.accumulo.core.util.shell.commands.ClasspathCommand;
import org.apache.accumulo.core.util.shell.commands.ClearCommand;
import org.apache.accumulo.core.util.shell.commands.CloneTableCommand;
import org.apache.accumulo.core.util.shell.commands.ClsCommand;
import org.apache.accumulo.core.util.shell.commands.CompactCommand;
import org.apache.accumulo.core.util.shell.commands.ConfigCommand;
import org.apache.accumulo.core.util.shell.commands.CreateTableCommand;
import org.apache.accumulo.core.util.shell.commands.CreateUserCommand;
import org.apache.accumulo.core.util.shell.commands.DUCommand;
import org.apache.accumulo.core.util.shell.commands.DebugCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteIterCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteManyCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteRowsCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteScanIterCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteTableCommand;
import org.apache.accumulo.core.util.shell.commands.DeleteUserCommand;
import org.apache.accumulo.core.util.shell.commands.DropTableCommand;
import org.apache.accumulo.core.util.shell.commands.DropUserCommand;
import org.apache.accumulo.core.util.shell.commands.EGrepCommand;
import org.apache.accumulo.core.util.shell.commands.ExecfileCommand;
import org.apache.accumulo.core.util.shell.commands.ExitCommand;
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
import org.apache.accumulo.core.util.shell.commands.InfoCommand;
import org.apache.accumulo.core.util.shell.commands.InsertCommand;
import org.apache.accumulo.core.util.shell.commands.ListIterCommand;
import org.apache.accumulo.core.util.shell.commands.ListScansCommand;
import org.apache.accumulo.core.util.shell.commands.MasterStateCommand;
import org.apache.accumulo.core.util.shell.commands.MaxRowCommand;
import org.apache.accumulo.core.util.shell.commands.MergeCommand;
import org.apache.accumulo.core.util.shell.commands.NoTableCommand;
import org.apache.accumulo.core.util.shell.commands.OfflineCommand;
import org.apache.accumulo.core.util.shell.commands.OnlineCommand;
import org.apache.accumulo.core.util.shell.commands.PasswdCommand;
import org.apache.accumulo.core.util.shell.commands.QuestionCommand;
import org.apache.accumulo.core.util.shell.commands.QuitCommand;
import org.apache.accumulo.core.util.shell.commands.QuotedStringTokenizer;
import org.apache.accumulo.core.util.shell.commands.RenameTableCommand;
import org.apache.accumulo.core.util.shell.commands.RevokeCommand;
import org.apache.accumulo.core.util.shell.commands.ScanCommand;
import org.apache.accumulo.core.util.shell.commands.SelectCommand;
import org.apache.accumulo.core.util.shell.commands.SelectrowCommand;
import org.apache.accumulo.core.util.shell.commands.SetAuthsCommand;
import org.apache.accumulo.core.util.shell.commands.SetGroupsCommand;
import org.apache.accumulo.core.util.shell.commands.SetIterCommand;
import org.apache.accumulo.core.util.shell.commands.SetScanIterCommand;
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
import org.apache.accumulo.core.zookeeper.ZooReader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A convenient console interface to perform basic accumulo functions Includes auto-complete, help, and quoted strings with escape sequences
 */
public class Shell {
  public static final Logger log = Logger.getLogger(Shell.class);
  private static final Logger audit = Logger.getLogger(Shell.class.getName() + ".audit");
  
  public static final int NO_FIXED_ARG_LENGTH_CHECK = -1;
  private static final String SHELL_DESCRIPTION = "Shell - Accumulo Interactive Shell";
  private static final String DEFAULT_AUTH_TIMEOUT = "60"; // in minutes
  
  private int exitCode = 0;
  private String tableName;
  private Instance instance;
  private Connector connector;
  private ConsoleReader reader;
  private AuthInfo credentials;
  private Class<? extends Formatter> defaultFormatterClass = DefaultFormatter.class;
  private Class<? extends Formatter> binaryFormatterClass = BinaryFormatter.class;
  public Map<String,List<IteratorSetting>> scanIteratorOptions = new HashMap<String,List<IteratorSetting>>();
  
  private Token rootToken;
  public final Map<String,Command> commandFactory = new TreeMap<String,Command>();
  private boolean configError = false;
  
  // Global options flags
  public static final String userOption = "u";
  public static final String tableOption = "t";
  public static final String helpOption = "?";
  public static final String helpLongOption = "help";
  
  // exit if true
  private boolean exit = false;
  
  // file to execute commands from
  private String execFile = null;
  // single command to execute from the command line
  private String execCommand = null;
  private boolean verbose = true;
  
  private boolean tabCompletion;
  private boolean disableAuthTimeout;
  private long authTimeout;
  private long lastUserActivity = System.currentTimeMillis();
  
  @SuppressWarnings("deprecation")
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
    
    Option execCommandOpt = new Option("e", "execute-command", true, "executes a command, and then exits");
    opts.addOption(execCommandOpt);
    
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
      instance = getDefaultInstance(AccumuloConfiguration.getSiteConfiguration());
    } else if (cl.hasOption(zooKeeperInstance.getOpt())) {
      String[] zkOpts = cl.getOptionValues(zooKeeperInstance.getOpt());
      instance = new ZooKeeperInstance(zkOpts[0], zkOpts[1]);
    } else {
      instance = getDefaultInstance(AccumuloConfiguration.getSiteConfiguration());
    }
    
    // process default parameters if unspecified
    byte[] pass;
    try {
      if (!cl.hasOption(fakeOption.getLongOpt())) {
        DistributedTrace.enable(instance, new ZooReader(instance), "shell", InetAddress.getLocalHost().getHostName());
      }
      
      this.reader = new ConsoleReader();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void start() {
          reader.getTerminal().enableEcho();
        }
      });
      
      if (passw == null)
        passw = reader.readLine("Enter current password for '" + user + "'@'" + instance.getInstanceName() + "': ", '*');
      if (passw == null) {
        reader.printNewline();
        configError = true;
        return;
      } // user canceled
      
      pass = passw.getBytes();
      this.setTableName("");
      connector = instance.getConnector(user, pass);
      this.credentials = new AuthInfo(user, ByteBuffer.wrap(pass), connector.getInstance().getInstanceID());
      
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
    Command external[] = {new AboutCommand(), new AddSplitsCommand(), new AuthenticateCommand(), new ByeCommand(), new ClasspathCommand(), new ClearCommand(),
        new CloneTableCommand(), new ClsCommand(), new CompactCommand(), new ConfigCommand(), new CreateTableCommand(), new CreateUserCommand(),
        new DebugCommand(), new DeleteCommand(), new DeleteIterCommand(), new DeleteManyCommand(), new DeleteRowsCommand(), new DeleteScanIterCommand(),
        new DeleteTableCommand(), new DeleteUserCommand(), new DropTableCommand(), new DropUserCommand(), new DUCommand(), new EGrepCommand(),
        new ExecfileCommand(), new ExitCommand(), new FlushCommand(), new FormatterCommand(), new GetAuthsCommand(), new GetGroupsCommand(),
        new GetSplitsCommand(), new GrantCommand(), new GrepCommand(), new HelpCommand(), new HiddenCommand(), new HistoryCommand(),
        new ImportDirectoryCommand(), new InfoCommand(), new InsertCommand(), new ListIterCommand(), new ListScansCommand(), new MasterStateCommand(),
        new MaxRowCommand(), new MergeCommand(), new NoTableCommand(), new OfflineCommand(), new OnlineCommand(), new PasswdCommand(), new QuestionCommand(),
        new QuitCommand(), new RenameTableCommand(), new RevokeCommand(), new ScanCommand(), new SelectCommand(), new SelectrowCommand(),
        new SetAuthsCommand(), new SetGroupsCommand(), new SetIterCommand(), new SetScanIterCommand(), new SleepCommand(), new SystemPermissionsCommand(),
        new TableCommand(), new TablePermissionsCommand(), new TablesCommand(), new TraceCommand(), new UserCommand(), new UserPermissionsCommand(),
        new UsersCommand(), new WhoAmICommand(),};
    for (Command cmd : external) {
      commandFactory.put(cmd.getName(), cmd);
    }
  }
  
  /**
   * @deprecated Not for client use
   */
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
    
    ShellCompletor userCompletor = null;
    
    if (execFile != null) {
      java.util.Scanner scanner = new java.util.Scanner(new File(execFile));
      while (scanner.hasNextLine())
        execCommand(scanner.nextLine(), true, isVerbose());
    } else if (execCommand != null) {
      for (String command : execCommand.split("\n")) {
        execCommand(command, true, isVerbose());
      }
      return exitCode;
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
  
  public void printInfo() throws IOException {
    reader.printString("\n" + SHELL_DESCRIPTION + "\n" + "- \n" + "- version: " + Constants.VERSION + "\n" + "- instance name: "
        + connector.getInstance().getInstanceName() + "\n" + "- instance id: " + connector.getInstance().getInstanceID() + "\n" + "- \n"
        + "- type 'help' for a list of available commands\n" + "- \n");
  }
  
  public void printVerboseInfo() throws IOException {
    StringBuilder sb = new StringBuilder("-\n");
    sb.append("- Current user: ").append(connector.whoami()).append("\n");
    if (execFile != null)
      sb.append("- Executing commands from: ").append(execFile).append("\n");
    if (disableAuthTimeout)
      sb.append("- Authorization timeout: disabled\n");
    else
      sb.append("- Authorization timeout: ").append(String.format("%.2fs\n", authTimeout / 1000.0));
    sb.append("- Debug: ").append(isDebuggingEnabled() ? "on" : "off").append("\n");
    if (!scanIteratorOptions.isEmpty()) {
      for (Entry<String,List<IteratorSetting>> entry : scanIteratorOptions.entrySet()) {
        sb.append("- Session scan iterators for table ").append(entry.getKey()).append(":\n");
        for (IteratorSetting setting : entry.getValue()) {
          sb.append("-    Iterator ").append(setting.getName()).append(" options:\n");
          sb.append("-        ").append("iteratorPriority").append(" = ").append(setting.getPriority()).append("\n");
          sb.append("-        ").append("iteratorClassName").append(" = ").append(setting.getIteratorClass()).append("\n");
          for (Entry<String,String> optEntry : setting.getProperties().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-\n");
    reader.printString(sb.toString());
  }
  
  private String getDefaultPrompt() {
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
          reader.printString(String.format("Unknown command \"%s\".  Enter \"help\" for a list possible commands.\n", command));
          return;
        }
        
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
    
    for (Command c : commandFactory.values()) {
      c.getOptionsWithHelp(); // prep the options for the command
      // so that the completion can
      // include them
      c.registerCompletion(rootToken, options);
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
    
    public final void printHelp(int width) {
      Shell.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp(), width);
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
  
  public static abstract class TableOperation extends Command {
    
    private Option optTablePattern, optTableName;
    
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
      
      String originalTable = "";
      
      if (shellState.getTableName() != "") {
        originalTable = shellState.getTableName();
      }
      
      if (cl.hasOption(optTableName.getOpt())) {
        String tableName = cl.getOptionValue(optTableName.getOpt());
        shellState.setTableName(tableName);
      }
      
      try {
        // populate the tablesToFlush set with the tables you want to flush
        SortedSet<String> tablesToFlush = new TreeSet<String>();
        if (cl.hasOption(optTablePattern.getOpt())) {
          for (String table : shellState.getConnector().tableOperations().list())
            if (table.matches(cl.getOptionValue(optTablePattern.getOpt())))
              tablesToFlush.add(table);
        } else if (cl.hasOption(optTableName.getOpt())) {
          tablesToFlush.add(cl.getOptionValue(optTableName.getOpt()));
        }
        
        else {
          shellState.checkTableState();
          tablesToFlush.add(shellState.getTableName());
        }
        
        if (tablesToFlush.isEmpty())
          log.warn("No tables found that match your criteria");
        
        // flush the tables
        for (String tableName : tablesToFlush) {
          if (!shellState.getConnector().tableOperations().exists(tableName))
            throw new TableNotFoundException(null, tableName, null);
          doTableOp(shellState, tableName);
        }
      }
      
      finally {
        shellState.setTableName(originalTable);
      }
      
      return 0;
    }
    
    protected abstract void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException;
    
    @Override
    public String description() {
      return "makes a best effort to flush tables from memory to disk";
    }
    
    @Override
    public Options getOptions() {
      Options o = new Options();
      
      optTablePattern = new Option("p", "pattern", true, "regex pattern of table names to flush");
      optTablePattern.setArgName("pattern");
      
      optTableName = new Option(tableOption, "table", true, "name of a table to flush");
      optTableName.setArgName("tableName");
      
      OptionGroup opg = new OptionGroup();
      
      opg.addOption(optTablePattern);
      opg.addOption(optTableName);
      
      o.addOptionGroup(opg);
      
      return o;
    }
    
    @Override
    public int numArgs() {
      return 0;
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
  
  public final void printLines(Iterator<String> lines, boolean paginate) throws IOException {
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
  
  public final void printRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate) throws IOException {
    Class<? extends Formatter> formatterClass = getFormatter();
    
    printRecords(scanner, printTimestamps, paginate, formatterClass);
  }
  
  public final void printRecords(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps, boolean paginate, Class<? extends Formatter> formatterClass)
      throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, printTimestamps), paginate);
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
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    log.error(String.format("%-" + COL1 + "s | %" + COL2 + "s | %-" + col3 + "s\n", "Constraint class", "Violation code", "Violation Description"));
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
    for (TConstraintViolationSummary cvs : cve.violationSummaries)
      log.error(String.format("%-" + COL1 + "s | %" + COL2 + "d | %-" + col3 + "s\n", cvs.constrainClass, cvs.violationCode, cvs.violationDescription));
    log.error(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s\n", repeat("-", COL1), repeat("-", COL2), repeat("-", col3)));
  }
  
  public static final void printException(Exception e) {
    printException(e, e.getMessage());
  }
  
  private static final void printException(Exception e, String msg) {
    log.error(e.getClass().getName() + (msg != null ? ": " + msg : ""));
    log.debug(e.getClass().getName() + (msg != null ? ": " + msg : ""), e);
  }
  
  public static final void setDebugging(boolean debuggingEnabled) {
    Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(debuggingEnabled ? Level.TRACE : Level.INFO);
  }
  
  public static final boolean isDebuggingEnabled() {
    return Logger.getLogger(Constants.CORE_PACKAGE_NAME).isTraceEnabled();
  }
  
  private static final void printHelp(String usage, String description, Options opts) {
    printHelp(usage, description, opts, Integer.MAX_VALUE);
  }
  
  private static final void printHelp(String usage, String description, Options opts, int width) {
    PrintWriter pw = new PrintWriter(System.err);
    new HelpFormatter().printHelp(pw, width, usage, description, opts, 2, 5, null, true);
    pw.flush();
  }
  
  public void setExit(boolean exit) {
    this.exit = exit;
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
  
  public void updateUser(AuthInfo authInfo) throws AccumuloException, AccumuloSecurityException {
    connector = instance.getConnector(authInfo);
    credentials = authInfo;
  }
  
  public AuthInfo getCredentials() {
    return credentials;
  }
  
  /**
   * Return the formatter for this table, .
   * 
   * @param tableName
   * @return The formatter class for the given table
   */
  public Class<? extends Formatter> getFormatter() {
    return getFormatter(this.tableName);
  }
  
  public Class<? extends Formatter> getFormatter(String tableName) {
    Class<? extends Formatter> formatter = FormatterCommand.getCurrentFormatter(tableName, this);
    
    if (null == formatter) {
      log.error("Could not load the specified formatter. Using the DefaultFormatter");
      return this.defaultFormatterClass;
    } else {
      return formatter;
    }
  }
}
