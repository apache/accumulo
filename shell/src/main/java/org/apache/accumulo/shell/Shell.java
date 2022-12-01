/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.cli.ClientOpts.PasswordConverter;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.core.util.format.FormatterFactory;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.shell.commands.AboutCommand;
import org.apache.accumulo.shell.commands.AddAuthsCommand;
import org.apache.accumulo.shell.commands.AddSplitsCommand;
import org.apache.accumulo.shell.commands.AuthenticateCommand;
import org.apache.accumulo.shell.commands.ByeCommand;
import org.apache.accumulo.shell.commands.ClasspathCommand;
import org.apache.accumulo.shell.commands.ClearCommand;
import org.apache.accumulo.shell.commands.CloneTableCommand;
import org.apache.accumulo.shell.commands.ClsCommand;
import org.apache.accumulo.shell.commands.CompactCommand;
import org.apache.accumulo.shell.commands.ConfigCommand;
import org.apache.accumulo.shell.commands.ConstraintCommand;
import org.apache.accumulo.shell.commands.CreateNamespaceCommand;
import org.apache.accumulo.shell.commands.CreateTableCommand;
import org.apache.accumulo.shell.commands.CreateUserCommand;
import org.apache.accumulo.shell.commands.DUCommand;
import org.apache.accumulo.shell.commands.DeleteAuthsCommand;
import org.apache.accumulo.shell.commands.DeleteCommand;
import org.apache.accumulo.shell.commands.DeleteIterCommand;
import org.apache.accumulo.shell.commands.DeleteManyCommand;
import org.apache.accumulo.shell.commands.DeleteNamespaceCommand;
import org.apache.accumulo.shell.commands.DeleteRowsCommand;
import org.apache.accumulo.shell.commands.DeleteShellIterCommand;
import org.apache.accumulo.shell.commands.DeleteTableCommand;
import org.apache.accumulo.shell.commands.DeleteUserCommand;
import org.apache.accumulo.shell.commands.DropTableCommand;
import org.apache.accumulo.shell.commands.DropUserCommand;
import org.apache.accumulo.shell.commands.EGrepCommand;
import org.apache.accumulo.shell.commands.ExecfileCommand;
import org.apache.accumulo.shell.commands.ExitCommand;
import org.apache.accumulo.shell.commands.ExportTableCommand;
import org.apache.accumulo.shell.commands.ExtensionCommand;
import org.apache.accumulo.shell.commands.FateCommand;
import org.apache.accumulo.shell.commands.FlushCommand;
import org.apache.accumulo.shell.commands.FormatterCommand;
import org.apache.accumulo.shell.commands.GetAuthsCommand;
import org.apache.accumulo.shell.commands.GetGroupsCommand;
import org.apache.accumulo.shell.commands.GetSplitsCommand;
import org.apache.accumulo.shell.commands.GrantCommand;
import org.apache.accumulo.shell.commands.GrepCommand;
import org.apache.accumulo.shell.commands.HelpCommand;
import org.apache.accumulo.shell.commands.HiddenCommand;
import org.apache.accumulo.shell.commands.HistoryCommand;
import org.apache.accumulo.shell.commands.ImportDirectoryCommand;
import org.apache.accumulo.shell.commands.ImportTableCommand;
import org.apache.accumulo.shell.commands.InfoCommand;
import org.apache.accumulo.shell.commands.InsertCommand;
import org.apache.accumulo.shell.commands.ListBulkCommand;
import org.apache.accumulo.shell.commands.ListCompactionsCommand;
import org.apache.accumulo.shell.commands.ListIterCommand;
import org.apache.accumulo.shell.commands.ListScansCommand;
import org.apache.accumulo.shell.commands.ListShellIterCommand;
import org.apache.accumulo.shell.commands.ListTabletsCommand;
import org.apache.accumulo.shell.commands.MaxRowCommand;
import org.apache.accumulo.shell.commands.MergeCommand;
import org.apache.accumulo.shell.commands.NamespacePermissionsCommand;
import org.apache.accumulo.shell.commands.NamespacesCommand;
import org.apache.accumulo.shell.commands.NoTableCommand;
import org.apache.accumulo.shell.commands.OfflineCommand;
import org.apache.accumulo.shell.commands.OnlineCommand;
import org.apache.accumulo.shell.commands.OptUtil;
import org.apache.accumulo.shell.commands.PasswdCommand;
import org.apache.accumulo.shell.commands.PingCommand;
import org.apache.accumulo.shell.commands.QuestionCommand;
import org.apache.accumulo.shell.commands.QuitCommand;
import org.apache.accumulo.shell.commands.QuotedStringTokenizer;
import org.apache.accumulo.shell.commands.RenameNamespaceCommand;
import org.apache.accumulo.shell.commands.RenameTableCommand;
import org.apache.accumulo.shell.commands.RevokeCommand;
import org.apache.accumulo.shell.commands.ScanCommand;
import org.apache.accumulo.shell.commands.SetAuthsCommand;
import org.apache.accumulo.shell.commands.SetGroupsCommand;
import org.apache.accumulo.shell.commands.SetIterCommand;
import org.apache.accumulo.shell.commands.SetShellIterCommand;
import org.apache.accumulo.shell.commands.SleepCommand;
import org.apache.accumulo.shell.commands.SummariesCommand;
import org.apache.accumulo.shell.commands.SystemPermissionsCommand;
import org.apache.accumulo.shell.commands.TableCommand;
import org.apache.accumulo.shell.commands.TablePermissionsCommand;
import org.apache.accumulo.shell.commands.TablesCommand;
import org.apache.accumulo.shell.commands.TraceCommand;
import org.apache.accumulo.shell.commands.UserCommand;
import org.apache.accumulo.shell.commands.UserPermissionsCommand;
import org.apache.accumulo.shell.commands.UsersCommand;
import org.apache.accumulo.shell.commands.WhoAmICommand;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A convenient console interface to perform basic accumulo functions Includes auto-complete, help,
 * and quoted strings with escape sequences
 */
@AutoService(KeywordExecutable.class)
public class Shell extends ShellOptions implements KeywordExecutable {
  public static final Logger log = LoggerFactory.getLogger(Shell.class);
  private static final Logger audit = LoggerFactory.getLogger(Shell.class.getName() + ".audit");

  private static final Predicate<String> IS_HELP_OPT =
      s -> s != null && (s.equals("-" + helpOption) || s.equals("--" + helpLongOption));
  public static final Charset CHARSET = ISO_8859_1;
  public static final int NO_FIXED_ARG_LENGTH_CHECK = -1;
  public static final String COMMENT_PREFIX = "#";
  public static final String HISTORY_DIR_NAME = ".accumulo";
  public static final String HISTORY_FILE_NAME = "shell_history.txt";
  private static final String SHELL_DESCRIPTION = "Shell - Apache Accumulo Interactive Shell";

  protected int exitCode = 0;
  private String tableName;
  private AccumuloClient accumuloClient;
  private Properties clientProperties = new Properties();
  private ClientContext context;
  protected LineReader reader;
  protected Terminal terminal;
  protected PrintWriter writer;
  private final Class<? extends Formatter> defaultFormatterClass = DefaultFormatter.class;
  public Map<String,List<IteratorSetting>> scanIteratorOptions = new HashMap<>();
  public Map<String,List<IteratorSetting>> iteratorProfiles = new HashMap<>();

  private Token rootToken;
  public final Map<String,Command> commandFactory = new TreeMap<>();
  public final Map<String,Command[]> commandGrouping = new TreeMap<>();

  // exit if true
  private boolean exit = false;

  // file to execute commands from
  protected File execFile = null;
  // single command to execute from the command line
  protected String execCommand = null;
  protected boolean verbose = true;

  private boolean tabCompletion;
  private boolean disableAuthTimeout;
  private long authTimeout;
  private long lastUserActivity = System.nanoTime();
  private boolean logErrorsToConsole = false;

  static {
    // set the JLine output encoding to some reasonable default if it isn't already set
    // despite the misleading property name, "input.encoding" is the property jline uses for the
    // encoding of the output stream writer
    String prop = "input.encoding";
    if (System.getProperty(prop) == null) {
      String value = System.getProperty("jline.WindowsTerminal.output.encoding");
      if (value == null) {
        value = System.getProperty("file.encoding");
      }
      if (value != null) {
        System.setProperty(prop, value);
      }
    }
  }

  // no arg constructor should do minimal work since its used in Main ServiceLoader
  public Shell() {}

  public Shell(LineReader reader) {
    this.reader = reader;
    this.terminal = reader.getTerminal();
    this.writer = terminal.writer();
  }

  /**
   * Configures the shell using the provided options. Not for client use.
   *
   * @return true if the shell was successfully configured, false otherwise.
   * @throws IOException if problems occur creating the LineReader
   */
  public boolean config(String... args) throws IOException {
    if (this.terminal == null) {
      this.terminal = TerminalBuilder.builder().jansi(false).build();
    }
    if (this.reader == null) {
      this.reader = LineReaderBuilder.builder().terminal(this.terminal).build();
    }
    this.writer = this.terminal.writer();

    ShellOptionsJC options = new ShellOptionsJC();
    JCommander jc = new JCommander();

    jc.setProgramName("accumulo shell");
    jc.addObject(options);
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      jc.usage();
      exitCode = 1;
      return false;
    }

    if (options.isHelpEnabled()) {
      jc.usage();
      // Not an error
      exitCode = 0;
      return false;
    }

    if (options.getUnrecognizedOptions() != null) {
      logError("Unrecognized Options: " + options.getUnrecognizedOptions());
      jc.usage();
      exitCode = 1;
      return false;
    }

    if (options.isDebugEnabled()) {
      log.warn("Configure debugging through your logging configuration file");
    }
    authTimeout = TimeUnit.MINUTES.toNanos(options.getAuthTimeout());
    disableAuthTimeout = options.isAuthTimeoutDisabled();

    clientProperties = options.getClientProperties();
    if (ClientProperty.SASL_ENABLED.getBoolean(clientProperties)) {
      log.debug("SASL is enabled, disabling authorization timeout");
      disableAuthTimeout = true;
    }

    tabCompletion = !options.isTabCompletionDisabled();
    this.setTableName("");

    if (accumuloClient == null) {
      if (ClientProperty.INSTANCE_ZOOKEEPERS.isEmpty(clientProperties)) {
        throw new IllegalArgumentException("ZooKeepers must be set using -z or -zh on command line"
            + " or in accumulo-client.properties");
      }
      if (ClientProperty.INSTANCE_NAME.isEmpty(clientProperties)) {
        throw new IllegalArgumentException("Instance name must be set using -z or -zi on command "
            + "line or in accumulo-client.properties");
      }
      final String principal;
      try {
        principal = options.getUsername();
      } catch (Exception e) {
        logError(e.getMessage());
        exitCode = 1;
        return false;
      }
      String password = options.getPassword();
      AuthenticationToken token = null;
      if (password == null && clientProperties.containsKey(ClientProperty.AUTH_TOKEN.getKey())
          && principal.equals(ClientProperty.AUTH_PRINCIPAL.getValue(clientProperties))) {
        token = ClientProperty.getAuthenticationToken(clientProperties);
      }
      if (token == null) {
        // Read password if the user explicitly asked for it, or didn't specify anything at all
        if (PasswordConverter.STDIN.equals(password) || password == null) {
          password = reader.readLine("Password: ", '*');
        }
        if (password == null) {
          // User cancel, e.g. Ctrl-D pressed
          throw new ParameterException("No password or token option supplied");
        } else {
          token = new PasswordToken(password);
        }
      }
      try {
        this.setTableName("");
        accumuloClient = Accumulo.newClient().from(clientProperties).as(principal, token).build();
        context = (ClientContext) accumuloClient;
      } catch (Exception e) {
        printException(e);
        exitCode = 1;
        return false;
      }
    }

    // decide whether to execute commands from a file and quit
    if (options.getExecFile() != null) {
      execFile = options.getExecFile();
      verbose = false;
    } else if (options.getExecFileVerbose() != null) {
      execFile = options.getExecFileVerbose();
      verbose = true;
    }
    execCommand = options.getExecCommand();
    if (execCommand != null) {
      verbose = false;
    }

    rootToken = new Token();

    @SuppressWarnings("deprecation")
    Command[] dataCommands = {new DeleteCommand(), new DeleteManyCommand(), new DeleteRowsCommand(),
        new EGrepCommand(), new FormatterCommand(),
        new org.apache.accumulo.shell.commands.InterpreterCommand(), new GrepCommand(),
        new ImportDirectoryCommand(), new InsertCommand(), new MaxRowCommand(), new ScanCommand()};
    @SuppressWarnings("deprecation")
    Command[] debuggingCommands =
        {new ClasspathCommand(), new org.apache.accumulo.shell.commands.DebugCommand(),
            new ListScansCommand(), new ListCompactionsCommand(), new TraceCommand(),
            new PingCommand(), new ListBulkCommand(), new ListTabletsCommand()};
    Command[] execCommands = {new ExecfileCommand(), new HistoryCommand(), new ExtensionCommand()};
    Command[] exitCommands = {new ByeCommand(), new ExitCommand(), new QuitCommand()};
    Command[] helpCommands =
        {new AboutCommand(), new HelpCommand(), new InfoCommand(), new QuestionCommand()};
    @SuppressWarnings("deprecation")
    Command[] iteratorCommands = {new DeleteIterCommand(),
        new org.apache.accumulo.shell.commands.DeleteScanIterCommand(), new ListIterCommand(),
        new SetIterCommand(), new org.apache.accumulo.shell.commands.SetScanIterCommand(),
        new SetShellIterCommand(), new ListShellIterCommand(), new DeleteShellIterCommand()};
    Command[] otherCommands = {new HiddenCommand()};
    Command[] permissionsCommands = {new GrantCommand(), new RevokeCommand(),
        new SystemPermissionsCommand(), new TablePermissionsCommand(), new UserPermissionsCommand(),
        new NamespacePermissionsCommand()};
    Command[] stateCommands = {new AuthenticateCommand(), new ClsCommand(), new ClearCommand(),
        new FateCommand(), new NoTableCommand(), new SleepCommand(), new TableCommand(),
        new UserCommand(), new WhoAmICommand()};
    Command[] tableCommands = {new CloneTableCommand(), new ConfigCommand(),
        new CreateTableCommand(), new DeleteTableCommand(), new DropTableCommand(), new DUCommand(),
        new ExportTableCommand(), new ImportTableCommand(), new OfflineCommand(),
        new OnlineCommand(), new RenameTableCommand(), new TablesCommand(), new NamespacesCommand(),
        new CreateNamespaceCommand(), new DeleteNamespaceCommand(), new RenameNamespaceCommand(),
        new SummariesCommand()};
    Command[] tableControlCommands = {new AddSplitsCommand(), new CompactCommand(),
        new ConstraintCommand(), new FlushCommand(), new GetGroupsCommand(), new GetSplitsCommand(),
        new MergeCommand(), new SetGroupsCommand()};
    Command[] userCommands = {new AddAuthsCommand(), new CreateUserCommand(),
        new DeleteUserCommand(), new DropUserCommand(), new GetAuthsCommand(), new PasswdCommand(),
        new SetAuthsCommand(), new UsersCommand(), new DeleteAuthsCommand()};
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
      for (Command cmd : cmds) {
        commandFactory.put(cmd.getName(), cmd);
      }
    }
    for (Command cmd : otherCommands) {
      commandFactory.put(cmd.getName(), cmd);
    }
    return true;
  }

  public AccumuloClient getAccumuloClient() {
    return accumuloClient;
  }

  public ClassLoader getClassLoader(final CommandLine cl, final Shell shellState)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {

    boolean tables =
        cl.hasOption(OptUtil.tableOpt().getOpt()) || !shellState.getTableName().isEmpty();
    boolean namespaces = cl.hasOption(OptUtil.namespaceOpt().getOpt());

    Map<String,String> tableProps;

    if (namespaces) {
      try {
        tableProps = shellState.getAccumuloClient().namespaceOperations()
            .getConfiguration(OptUtil.getNamespaceOpt(cl, shellState));
      } catch (NamespaceNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (tables) {
      tableProps = shellState.getAccumuloClient().tableOperations()
          .getConfiguration(OptUtil.getTableOpt(cl, shellState));
    } else {
      throw new IllegalArgumentException("No table or namespace specified");
    }
    String tableContext = getTableContextFromProps(tableProps);

    if (tableContext != null && !tableContext.isEmpty()) {
      ClassLoaderUtil.initContextFactory(new ConfigurationCopy(
          shellState.getAccumuloClient().instanceOperations().getSystemConfiguration()));
    }
    return ClassLoaderUtil.getClassLoader(tableContext);
  }

  private static String getTableContextFromProps(Map<String,String> props) {
    String tableContext = null;
    for (Entry<String,String> entry : props.entrySet()) {
      // look for either the old property or the new one, but
      // if the new one is set, stop looking and let it take precedence
      if (entry.getKey().equals(Property.TABLE_CLASSLOADER_CONTEXT.getKey())
          && entry.getValue() != null && !entry.getValue().isEmpty()) {
        return entry.getValue();
      }
      @SuppressWarnings("removal")
      Property TABLE_CLASSPATH = Property.TABLE_CLASSPATH;
      if (entry.getKey().equals(TABLE_CLASSPATH.getKey())) {
        // don't return even if this is set; instead,
        // keep looking, in case we find the newer property set
        tableContext = entry.getValue();
        if (tableContext != null && !tableContext.isEmpty()) {
          log.warn("Deprecated table context property detected. '{}' should be replaced by '{}'",
              TABLE_CLASSPATH.getKey(), Property.TABLE_CLASSLOADER_CONTEXT.getKey());
        }
      }
    }
    return tableContext;
  }

  @Override
  public String keyword() {
    return "shell";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Runs Accumulo shell";
  }

  @SuppressFBWarnings(value = "DM_EXIT", justification = "System.exit() from a main class is okay")
  @Override
  public void execute(final String[] args) throws IOException {
    try {
      if (!config(args)) {
        System.exit(getExitCode());
      }

      System.exit(start());
    } finally {
      shutdown();
    }
  }

  public static void main(String[] args) throws IOException {
    LineReader reader = LineReaderBuilder.builder().build();
    new Shell(reader).execute(args);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "user-provided paths intentional")
  public int start() throws IOException {
    String input;
    if (isVerbose()) {
      printInfo();
    }

    String home = System.getProperty("HOME");
    if (home == null) {
      home = System.getenv("HOME");
    }
    String configDir = home + "/" + HISTORY_DIR_NAME;
    String historyPath = configDir + "/" + HISTORY_FILE_NAME;
    File accumuloDir = new File(configDir);
    if (!accumuloDir.exists() && !accumuloDir.mkdirs()) {
      log.warn("Unable to make directory for history at {}", accumuloDir);
    }

    // Remove Timestamps for history file. Fixes incompatibility issues
    reader.unsetOpt(LineReader.Option.HISTORY_TIMESTAMPED);

    // Set history file
    reader.setVariable(LineReader.HISTORY_FILE, new File(historyPath));

    // Turn Ctrl+C into Exception when trying to cancel a command instead of JVM exit
    Thread executeThread = Thread.currentThread();
    terminal.handle(Terminal.Signal.INT, signal -> executeThread.interrupt());

    ShellCompletor userCompletor;

    if (execFile != null) {
      try (java.util.Scanner scanner = new java.util.Scanner(execFile, UTF_8)) {
        while (scanner.hasNextLine() && !hasExited()) {
          execCommand(scanner.nextLine(), true, isVerbose());
        }
      }
    } else if (execCommand != null) {
      for (String command : execCommand.split("\n")) {
        execCommand(command, true, isVerbose());
      }
      return exitCode;
    }

    while (true) {
      try {
        if (hasExited()) {
          return exitCode;
        }

        // If tab completion is true we need to reset
        if (tabCompletion) {
          userCompletor = setupCompletion();
          ((LineReaderImpl) reader).setCompleter(userCompletor);
        }

        input = reader.readLine(getDefaultPrompt());

        execCommand(input, disableAuthTimeout, false);
      } catch (UserInterruptException uie) {
        // User Cancelled (Ctrl+C)
        writer.println();

        String partialLine = uie.getPartialLine();
        if (partialLine == null || "".equals(uie.getPartialLine().trim())) {
          // No content, actually exit
          return exitCode;
        }
      } catch (EndOfFileException efe) {
        // User Canceled (Ctrl+D)
        writer.println();
        return exitCode;
      } finally {
        writer.flush();
      }
    }
  }

  public void shutdown() {
    if (reader != null) {
      try {
        terminal.close();
        reader = null;
      } catch (IOException e) {
        printException(e);
      }
    }
    if (accumuloClient != null) {
      accumuloClient.close();
    }
  }

  public void printInfo() {
    ClientInfo info = ClientInfo.from(accumuloClient.properties());
    writer.print("\n" + SHELL_DESCRIPTION + "\n- \n- version: " + Constants.VERSION + "\n"
        + "- instance name: " + info.getInstanceName() + "\n- instance id: "
        + accumuloClient.instanceOperations().getInstanceId() + "\n- \n"
        + "- type 'help' for a list of available commands\n- \n");
    writer.flush();
  }

  public void printVerboseInfo() {
    StringBuilder sb = new StringBuilder("-\n");
    sb.append("- Current user: ").append(accumuloClient.whoami()).append("\n");
    if (execFile != null) {
      sb.append("- Executing commands from: ").append(execFile).append("\n");
    }
    if (disableAuthTimeout) {
      sb.append("- Authorization timeout: disabled\n");
    } else {
      sb.append("- Authorization timeout: ")
          .append(String.format("%ds%n", TimeUnit.NANOSECONDS.toSeconds(authTimeout)));
    }
    if (!scanIteratorOptions.isEmpty()) {
      for (Entry<String,List<IteratorSetting>> entry : scanIteratorOptions.entrySet()) {
        sb.append("- Session scan iterators for table ").append(entry.getKey()).append(":\n");
        for (IteratorSetting setting : entry.getValue()) {
          sb.append("-    Iterator ").append(setting.getName()).append(" options:\n");
          sb.append("-        ").append("iteratorPriority").append(" = ")
              .append(setting.getPriority()).append("\n");
          sb.append("-        ").append("iteratorClassName").append(" = ")
              .append(setting.getIteratorClass()).append("\n");
          for (Entry<String,String> optEntry : setting.getOptions().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ")
                .append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-\n");
    writer.print(sb);
  }

  public String getDefaultPrompt() {
    Objects.requireNonNull(accumuloClient);
    ClientInfo info = ClientInfo.from(accumuloClient.properties());
    return accumuloClient.whoami() + "@" + info.getInstanceName()
        + (getTableName().isEmpty() ? "" : " ") + getTableName() + "> ";
  }

  /**
   * Prevent potential CRLF injection into logs from read in user data. See the
   * <a href="https://find-sec-bugs.github.io/bugs.htm#CRLF_INJECTION_LOGS">bug description</a>
   */
  private String sanitize(String msg) {
    return msg.replaceAll("[\r\n]", "");
  }

  public void execCommand(String input, boolean ignoreAuthTimeout, boolean echoPrompt) {
    audit.info("{}", sanitize(getDefaultPrompt() + input));
    if (echoPrompt) {
      writer.print(getDefaultPrompt());
      writer.println(input);
    }

    if (input.startsWith(COMMENT_PREFIX)) {
      return;
    }

    String[] fields;
    try {
      fields = new QuotedStringTokenizer(input).getTokens();
    } catch (BadArgumentException e) {
      printException(e);
      ++exitCode;
      return;
    }
    if (fields.length == 0) {
      return;
    }

    String command = fields[0];
    fields = fields.length > 1 ? Arrays.copyOfRange(fields, 1, fields.length) : new String[] {};

    Command sc = null;
    if (command.isEmpty()) {
      ++exitCode;
      printException(new BadArgumentException("Unrecognized empty command", command, -1));
    } else {
      try {
        // Obtain the command from the command table
        sc = commandFactory.get(command);
        if (sc == null) {
          writer.println(String.format(
              "Unknown command \"%s\".  Enter \"help\" for a list possible commands.", command));
          writer.flush();
          return;
        }

        long duration = System.nanoTime() - lastUserActivity;
        if (!(sc instanceof ExitCommand) && !ignoreAuthTimeout
            && (duration < 0 || duration > authTimeout)) {
          writer.println("Shell has been idle for too long. Please re-authenticate.");
          boolean authFailed = true;
          do {
            String pwd = readMaskedLine(
                "Enter current password for '" + accumuloClient.whoami() + "': ", '*');
            if (pwd == null) {
              writer.println();
              return;
            } // user canceled

            try {
              authFailed = !accumuloClient.securityOperations()
                  .authenticateUser(accumuloClient.whoami(), new PasswordToken(pwd));
            } catch (Exception e) {
              ++exitCode;
              printException(e);
            }

            if (authFailed) {
              writer.print("Invalid password. ");
            }
          } while (authFailed);
          lastUserActivity = System.nanoTime();
        }

        // Get the options from the command on how to parse the string
        Options parseOpts = sc.getOptionsWithHelp();

        // Parse the string using the given options
        CommandLine cl = new DefaultParser().parse(parseOpts, fields);

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
          printException(new IllegalArgumentException(String.format(
              "Expected %d argument%s. There %s %d.", expectedArgLen,
              expectedArgLen == 1 ? "" : "s", actualArgLen == 1 ? "was" : "were", actualArgLen)));
          sc.printHelp(this);
        } else {
          int tmpCode = sc.execute(input, cl, this);
          exitCode += tmpCode;
          writer.flush();
        }

      } catch (ConstraintViolationException e) {
        ++exitCode;
        printConstraintViolationException(e);
      } catch (TableNotFoundException e) {
        ++exitCode;
        if (getTableName().equals(e.getTableName())) {
          setTableName("");
        }
        printException(e);
      } catch (ParseException e) {
        if (e instanceof MissingOptionException && Stream.of(fields).anyMatch(IS_HELP_OPT)) {
          // not really an error if the exception shows a required option is missing
          // and the user is asking for help
        } else {
          ++exitCode;
          printException(e);
        }
        sc.printHelp(this);
      } catch (UserInterruptException e) {
        ++exitCode;
      } catch (Exception e) {
        ++exitCode;
        printException(e);
      }
    }
    writer.flush();
  }

  /**
   * The command tree is built in reverse so that the references are more easily linked up. There is
   * some code in token to allow forward building of the command tree.
   */
  private ShellCompletor setupCompletion() {
    rootToken = new Token();

    Set<String> tableNames;
    try {
      tableNames = accumuloClient.tableOperations().list();
    } catch (Exception e) {
      log.debug("Unable to obtain list of tables", e);
      tableNames = Collections.emptySet();
    }

    Set<String> userlist;
    try {
      userlist = accumuloClient.securityOperations().listLocalUsers();
    } catch (Exception e) {
      log.debug("Unable to obtain list of users", e);
      userlist = Collections.emptySet();
    }

    Set<String> namespaces;
    try {
      namespaces = accumuloClient.namespaceOperations().list();
    } catch (Exception e) {
      log.debug("Unable to obtain list of namespaces", e);
      namespaces = Collections.emptySet();
    }

    Map<Command.CompletionSet,Set<String>> options = new HashMap<>();

    Set<String> commands = new HashSet<>(commandFactory.keySet());
    Set<String> modifiedUserlist = new HashSet<>();
    Set<String> modifiedTablenames = new HashSet<>();
    Set<String> modifiedNamespaces = new HashSet<>();

    for (String a : tableNames) {
      modifiedTablenames.add(a.replaceAll("([\\s'\"])", "\\\\$1"));
    }
    for (String a : userlist) {
      modifiedUserlist.add(a.replaceAll("([\\s'\"])", "\\\\$1"));
    }
    for (String a : namespaces) {
      String b = a.replaceAll("([\\s'\"])", "\\\\$1");
      modifiedNamespaces.add(b.isEmpty() ? "\"\"" : b);
    }

    options.put(Command.CompletionSet.USERNAMES, modifiedUserlist);
    options.put(Command.CompletionSet.TABLENAMES, modifiedTablenames);
    options.put(Command.CompletionSet.NAMESPACES, modifiedNamespaces);
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
   * The Command class represents a command to be run in the shell. It contains the methods to
   * execute along with some methods to help tab completion, and return the command name, help, and
   * usage.
   */
  public abstract static class Command {
    // Helper methods for completion
    public enum CompletionSet {
      TABLENAMES, USERNAMES, COMMANDS, NAMESPACES
    }

    public void registerCompletionGeneral(Token root, Set<String> args, boolean caseSens) {
      Token t = new Token(args);
      t.setCaseSensitive(caseSens);

      Token command = new Token(getName());
      command.addSubcommand(t);

      root.addSubcommand(command);
    }

    public void registerCompletionForTables(Token root,
        Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.TABLENAMES), true);
    }

    public void registerCompletionForUsers(Token root,
        Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.USERNAMES), true);
    }

    public void registerCompletionForCommands(Token root,
        Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.COMMANDS), false);
    }

    public void registerCompletionForNamespaces(Token root,
        Map<CompletionSet,Set<String>> completionSet) {
      registerCompletionGeneral(root, completionSet.get(CompletionSet.NAMESPACES), true);
    }

    // abstract methods to override
    public abstract int execute(String fullCommand, CommandLine cl, Shell shellState)
        throws Exception;

    public abstract String description();

    /**
     * If the number of arguments is not always zero (not including those arguments handled through
     * Options), make sure to override the {@link #usage()} method. Otherwise, {@link #usage()} does
     * need to be overridden.
     */
    public abstract int numArgs();

    // OPTIONAL methods to override:

    // the general version of getName uses reflection to get the class name
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
      shellState.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp(),
          width);
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

  public interface PrintLine extends AutoCloseable {
    void print(String s);

    @Override
    void close();
  }

  public static class PrintShell implements PrintLine {
    LineReader reader;

    public PrintShell(LineReader reader) {
      this.reader = reader;
    }

    @Override
    public void print(String s) {
      try {
        reader.getTerminal().writer().println(s);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void close() {}
  }

  public static class PrintFile implements PrintLine {
    PrintWriter writer;

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_OUT",
        justification = "app is run in same security context as user providing the filename")
    public PrintFile(String filename) throws FileNotFoundException {
      writer = new PrintWriter(
          new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), UTF_8)));
    }

    @Override
    public void print(String s) {
      writer.println(s);
    }

    @Override
    public void close() {
      writer.close();
    }
  }

  public final void printLines(Iterator<String> lines, boolean paginate) throws IOException {
    printLines(lines, paginate, null);
  }

  public final void printLines(Iterator<String> lines, boolean paginate, PrintLine out)
      throws IOException {
    double linesPrinted = 0;
    String prompt = "-- hit any key to continue or 'q' to quit --";
    int lastPromptLength = prompt.length();
    int termWidth = terminal.getWidth();
    int maxLines = terminal.getHeight();

    String peek = null;
    while (lines.hasNext()) {
      String nextLine = lines.next();
      if (nextLine == null) {
        continue;
      }
      for (String line : nextLine.split("\\n")) {
        if (out == null) {
          if (peek != null) {
            writer.println(peek);
            if (paginate) {
              linesPrinted += peek.isEmpty() ? 0 : Math.ceil(peek.length() * 1.0 / termWidth);

              // check if displaying the next line would result in
              // scrolling off the screen
              if (linesPrinted + Math.ceil(lastPromptLength * 1.0 / termWidth)
                  + Math.ceil(prompt.length() * 1.0 / termWidth)
                  + Math.ceil(line.length() * 1.0 / termWidth) > maxLines) {
                linesPrinted = 0;
                int numdashes = (termWidth - prompt.length()) / 2;
                String nextPrompt = repeat("-", numdashes) + prompt + repeat("-", numdashes);
                lastPromptLength = nextPrompt.length();
                writer.print(nextPrompt);
                writer.flush();

                // Enter raw mode so character can be read without hitting 'enter' after
                Attributes attr = terminal.enterRawMode();
                int c = terminal.reader().read();
                // Resets raw mode
                terminal.setAttributes(attr);
                if (Character.toUpperCase((char) c) == 'Q') {
                  writer.println();
                  return;
                }
                writer.println();
                termWidth = terminal.getWidth();
                maxLines = terminal.getHeight();
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
      writer.println(peek);
    }
  }

  public final void printRecords(Iterable<Entry<Key,Value>> scanner, FormatterConfig config,
      boolean paginate, Class<? extends Formatter> formatterClass, PrintLine outFile)
      throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, config), paginate, outFile);
  }

  public final void printRecords(Iterable<Entry<Key,Value>> scanner, FormatterConfig config,
      boolean paginate, Class<? extends Formatter> formatterClass) throws IOException {
    printLines(FormatterFactory.getFormatter(formatterClass, scanner, config), paginate);
  }

  public static String repeat(String s, int c) {
    return s.repeat(Math.max(0, c));
  }

  public void checkTableState() {
    if (getTableName().isEmpty()) {
      throw new IllegalStateException("Not in a table context. Please use"
          + " 'table <tableName>' to switch to a table, or use '-t' to specify a"
          + " table if option is available.");
    }
  }

  private void printConstraintViolationException(ConstraintViolationException cve) {
    printException(cve, "");
    int COL1 = 50, COL2 = 14;
    int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, terminal.getWidth() - COL1 - COL2 - 6));
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1),
        repeat("-", COL2), repeat("-", col3)));
    logError(String.format("%-" + COL1 + "s | %" + COL2 + "s | %-" + col3 + "s%n",
        "Constraint class", "Violation code", "Violation Description"));
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1),
        repeat("-", COL2), repeat("-", col3)));
    for (TConstraintViolationSummary cvs : cve.violationSummaries) {
      logError(String.format("%-" + COL1 + "s | %" + COL2 + "d | %-" + col3 + "s%n",
          cvs.constrainClass, cvs.violationCode, cvs.violationDescription));
    }
    logError(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%" + col3 + "s%n", repeat("-", COL1),
        repeat("-", COL2), repeat("-", col3)));
  }

  public final void printException(Exception e) {
    printException(e, e.getMessage());
  }

  private void printException(Exception e, String msg) {
    logError(e.getClass().getName() + (msg != null ? ": " + msg : ""));
    log.debug("{}{}", e.getClass().getName(), msg != null ? ": " + msg : "", e);
  }

  private void printHelp(String usage, String description, Options opts) {
    printHelp(usage, description, opts, Integer.MAX_VALUE);
  }

  private void printHelp(String usage, String description, Options opts, int width) {
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
    this.tableName =
        (tableName == null || tableName.isEmpty()) ? "" : TableNameUtil.qualified(tableName);
  }

  public String getTableName() {
    return tableName;
  }

  public LineReader getReader() {
    return reader;
  }

  public Terminal getTerminal() {
    return terminal;
  }

  public PrintWriter getWriter() {
    return writer;
  }

  public void updateUser(String principal, AuthenticationToken token)
      throws AccumuloException, AccumuloSecurityException {
    var newClient = Accumulo.newClient().from(clientProperties).as(principal, token).build();
    try {
      newClient.securityOperations().authenticateUser(principal, token);
    } catch (AccumuloSecurityException e) {
      // new client can't authenticate; close and discard
      newClient.close();
      throw e;
    }
    var oldClient = accumuloClient;
    accumuloClient = newClient; // swap out old client if the new client has authenticated
    context = (ClientContext) accumuloClient; // update the context with the new client
    if (oldClient != null) {
      // clean up the old client
      oldClient.close();
    }
  }

  public ClientContext getContext() {
    return context;
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
   * @param tableName the table name
   * @return the formatter class for the given table
   */
  public Class<? extends Formatter> getFormatter(String tableName) {
    Class<? extends Formatter> formatter = FormatterCommand.getCurrentFormatter(tableName, this);

    if (formatter == null) {
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
    log.error("{}", s);
    if (logErrorsToConsole) {
      writer.println("ERROR: " + s);
      writer.flush();
    }
  }

  public String readMaskedLine(String prompt, Character mask) {
    return reader.readLine(prompt, mask);
  }

  public boolean hasExited() {
    return exit;
  }

}
