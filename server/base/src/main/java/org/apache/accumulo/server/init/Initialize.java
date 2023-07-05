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
package org.apache.accumulo.server.init;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.TABLE_DIR;
import static org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode.BAD_CREDENTIALS;
import static org.apache.hadoop.fs.Path.SEPARATOR;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.ChangeSecret;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is used to set up the directory structure and the root tablet to get an instance
 * started
 */
@SuppressFBWarnings(value = "DM_EXIT", justification = "CLI utility can exit")
@AutoService(KeywordExecutable.class)
public class Initialize implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(Initialize.class);
  private static final String DEFAULT_ROOT_USER = "root";

  static void checkInit(ZooReaderWriter zoo, VolumeManager fs, InitialConfiguration initConfig)
      throws IOException {
    var hadoopConf = initConfig.getHadoopConf();
    log.info("Hadoop Filesystem is {}", FileSystem.getDefaultUri(hadoopConf));
    log.info("Accumulo data dirs are {}", List.of(initConfig.getVolumeUris()));
    log.info("Zookeeper server is {}", initConfig.get(Property.INSTANCE_ZK_HOST));
    log.info("Checking if Zookeeper is available. If this hangs, then you need"
        + " to make sure zookeeper is running");
    if (!zookeeperAvailable(zoo)) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      throw new IllegalStateException(
          "FATAL Zookeeper needs to be up and running in order to init. Exiting ...");
    }
    if (initConfig.get(Property.INSTANCE_SECRET)
        .equals(Property.INSTANCE_SECRET.getDefaultValue())) {

      System.out.println();
      System.out.println();
      System.out.println("Warning!!! Your instance secret is still set to the default,"
          + " this is not secure. We highly recommend you change it.");
      System.out.println();
      System.out.println();
      System.out.println("You can change the instance secret in accumulo by using:");
      System.out.println("   bin/accumulo " + ChangeSecret.class.getName());
      System.out.println("You will also need to edit your secret in your configuration"
          + " file by adding the property instance.secret to your"
          + " accumulo.properties. Without this accumulo will not operate correctly");
    }

    if (isInitialized(fs, initConfig)) {
      printInitializeFailureMessages(initConfig);
      throw new IOException("Filesystem is already initialized");
    }
  }

  private static void printInitializeFailureMessages(InitialConfiguration initConfig) {
    log.error("It appears the directories {}",
        initConfig.getVolumeUris() + " were previously initialized.");
    log.error("Change the property {} to use different volumes.",
        Property.INSTANCE_VOLUMES.getKey());
    log.error("The current value of {} is |{}|", Property.INSTANCE_VOLUMES.getKey(),
        initConfig.get(Property.INSTANCE_VOLUMES));
  }

  private boolean doInit(ZooReaderWriter zoo, Opts opts, VolumeManager fs,
      InitialConfiguration initConfig) {
    String instanceNamePath;
    String instanceName;
    String rootUser;

    try {
      checkInit(zoo, fs, initConfig);

      // prompt user for instance name and root password early, in case they
      // abort, we don't leave an inconsistent HDFS/ZooKeeper structure
      instanceNamePath = getInstanceNamePath(zoo, opts);
      rootUser = getRootUserName(initConfig, opts);

      // Don't prompt for a password when we're running SASL(Kerberos)
      if (initConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        opts.rootpass = UUID.randomUUID().toString().getBytes(UTF_8);
      } else {
        opts.rootpass = getRootPassword(initConfig, opts, rootUser);
      }

      // the actual disk locations of the root table and tablets
      instanceName = instanceNamePath.substring(getInstanceNamePrefix().length());
    } catch (Exception e) {
      log.error("FATAL: Problem during initialize", e);
      return false;
    }

    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ZooKeeperInitializer zki = new ZooKeeperInitializer();
    zki.initializeConfig(instanceId, zoo);

    try (ServerContext context =
        ServerContext.initialize(initConfig.getSiteConf(), instanceName, instanceId)) {
      var chooserEnv = new VolumeChooserEnvironmentImpl(Scope.INIT, RootTable.ID, null, context);
      String rootTabletDirName = RootTable.ROOT_TABLET_DIR_NAME;
      String ext = FileOperations.getNewFileExtension(DefaultConfiguration.getInstance());
      String rootTabletFileUri = new Path(
          fs.choose(chooserEnv, initConfig.getVolumeUris()) + SEPARATOR + TABLE_DIR + SEPARATOR
              + RootTable.ID + SEPARATOR + rootTabletDirName + SEPARATOR + "00000_00000." + ext)
          .toString();
      zki.initialize(context, opts.clearInstanceName, instanceNamePath, rootTabletDirName,
          rootTabletFileUri);

      if (!createDirs(fs, instanceId, initConfig.getVolumeUris())) {
        throw new IOException("Problem creating directories on " + fs.getVolumes());
      }
      var fileSystemInitializer = new FileSystemInitializer(initConfig, zoo, instanceId);
      var rootVol = fs.choose(chooserEnv, initConfig.getVolumeUris());
      var rootPath = new Path(rootVol + SEPARATOR + TABLE_DIR + SEPARATOR + RootTable.ID + SEPARATOR
          + rootTabletDirName);
      fileSystemInitializer.initialize(fs, rootPath.toString(), rootTabletFileUri, context);

      checkSASL(initConfig);
      initSecurity(context, opts, rootUser);

      checkUploadProps(context, initConfig, opts);
    } catch (Exception e) {
      log.error("FATAL: Problem during initialize", e);
      return false;
    }
    return true;
  }

  private void checkUploadProps(ServerContext context, InitialConfiguration initConfig, Opts opts) {
    if (opts.uploadAccumuloProps) {
      log.info("Uploading properties in accumulo.properties to Zookeeper."
          + " Properties that cannot be set in Zookeeper will be skipped:");
      Map<String,String> entries = new TreeMap<>();
      initConfig.getProperties(entries, x -> true, false);
      for (Map.Entry<String,String> entry : entries.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (Property.isValidZooPropertyKey(key)) {
          SystemPropUtil.setSystemProperty(context, key, value);
          log.info("Uploaded - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
        } else {
          log.info("Skipped - {} = {}", key, Property.isSensitive(key) ? "<hidden>" : value);
        }
      }
    }
  }

  /**
   * When we're using Kerberos authentication, we need valid credentials to perform initialization.
   * If the user provided some, use them. If they did not, fall back to the credentials present in
   * accumulo.properties that the servers will use themselves.
   */
  private void checkSASL(InitialConfiguration initConfig)
      throws IOException, AccumuloSecurityException {
    if (initConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      // We don't have any valid creds to talk to HDFS
      if (!ugi.hasKerberosCredentials()) {
        final String accumuloKeytab = initConfig.get(Property.GENERAL_KERBEROS_KEYTAB),
            accumuloPrincipal = initConfig.get(Property.GENERAL_KERBEROS_PRINCIPAL);

        // Fail if the site configuration doesn't contain appropriate credentials
        if (StringUtils.isBlank(accumuloKeytab) || StringUtils.isBlank(accumuloPrincipal)) {
          log.error("FATAL: No Kerberos credentials provided, and Accumulo is"
              + " not properly configured for server login");
          throw new AccumuloSecurityException(ugi.getUserName(), BAD_CREDENTIALS);
        }

        log.info("Logging in as {} with {}", accumuloPrincipal, accumuloKeytab);

        // Login using the keytab as the 'accumulo' user
        UserGroupInformation.loginUserFromKeytab(accumuloPrincipal, accumuloKeytab);
      }
    }
  }

  private static boolean zookeeperAvailable(ZooReaderWriter zoo) {
    try {
      return zoo.exists("/");
    } catch (KeeperException | InterruptedException e) {
      return false;
    }
  }

  /**
   * Create the version directory and the instance id path and file. The method tries to create the
   * directories and instance id file for all base directories provided unless an IOException is
   * thrown. If an IOException occurs, this method won't retry and will return false.
   *
   * @return false if an IOException occurred, true otherwise.
   */
  private static boolean createDirs(VolumeManager fs, InstanceId instanceId, Set<String> baseDirs) {
    boolean success;

    try {
      for (String baseDir : baseDirs) {
        log.debug("creating instance directories for base: {}", baseDir);

        Path verDir =
            new Path(new Path(baseDir, Constants.VERSION_DIR), "" + AccumuloDataVersion.get());
        FsPermission permission = new FsPermission("700");

        if (fs.exists(verDir)) {
          FileStatus fsStat = fs.getFileStatus(verDir);
          log.info("directory {} exists. Permissions match: {}", fsStat.getPath(),
              fsStat.getPermission().equals(permission));
        } else {
          success = fs.mkdirs(verDir, permission);
          log.info("Directory {} created - call returned {}", verDir, success);
        }

        Path iidLocation = new Path(baseDir, Constants.INSTANCE_ID_DIR);
        if (fs.exists(iidLocation)) {
          log.info("directory {} exists.", iidLocation);
        } else {
          success = fs.mkdirs(iidLocation);
          log.info("Directory {} created - call returned {}", iidLocation, success);
        }

        Path iidPath = new Path(iidLocation, instanceId.canonical());

        if (fs.exists(iidPath)) {
          log.info("InstanceID file {} exists.", iidPath);
        } else {
          success = fs.createNewFile(iidPath);
          // the exists() call provides positive check that the instanceId file is present
          if (success && fs.exists(iidPath)) {
            log.info("Created instanceId file {} in hdfs", iidPath);
          } else {
            log.warn("May have failed to create instanceId file {} in hdfs", iidPath);
          }
        }
      }
      return true;
    } catch (IOException e) {
      log.error("Problem creating new directories", e);
      return false;
    }
  }

  private String getInstanceNamePrefix() {
    return Constants.ZROOT + Constants.ZINSTANCES + "/";
  }

  private String getInstanceNamePath(ZooReaderWriter zoo, Opts opts)
      throws KeeperException, InterruptedException {
    // set up the instance name
    String instanceName, instanceNamePath = null;
    boolean exists = true;
    do {
      if (opts.cliInstanceName == null) {
        instanceName = System.console().readLine("Instance name : ");
      } else {
        instanceName = opts.cliInstanceName;
      }
      if (instanceName == null) {
        System.exit(0);
      }
      instanceName = instanceName.trim();
      if (instanceName.isEmpty()) {
        continue;
      }
      instanceNamePath = getInstanceNamePrefix() + instanceName;
      if (opts.clearInstanceName) {
        exists = false;
      } else {
        // ACCUMULO-4401 setting exists=false is just as important as setting it to true
        exists = zoo.exists(instanceNamePath);
        if (exists) {
          String decision = System.console().readLine("Instance name \"" + instanceName
              + "\" exists. Delete existing entry from zookeeper? [Y/N] : ");
          if (decision == null) {
            System.exit(0);
          }
          if (decision.length() == 1 && decision.toLowerCase(Locale.ENGLISH).charAt(0) == 'y') {
            opts.clearInstanceName = true;
            exists = false;
          }
        }
      }
    } while (exists);
    return instanceNamePath;
  }

  private String getRootUserName(InitialConfiguration initConfig, Opts opts) {
    final String keytab = initConfig.get(Property.GENERAL_KERBEROS_KEYTAB);
    if (keytab.equals(Property.GENERAL_KERBEROS_KEYTAB.getDefaultValue())
        || !initConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return DEFAULT_ROOT_USER;
    }

    System.out.println("Running against secured HDFS");

    if (opts.rootUser != null) {
      return opts.rootUser;
    }

    do {
      String user =
          System.console().readLine("Principal (user) to grant administrative privileges to : ");
      if (user == null) {
        // should not happen
        System.exit(1);
      }
      if (!user.isEmpty()) {
        return user;
      }
    } while (true);
  }

  private byte[] getRootPassword(InitialConfiguration initConfig, Opts opts, String rootUser) {
    if (opts.cliPassword != null) {
      return opts.cliPassword.getBytes(UTF_8);
    }
    String strrootpass;
    String strconfirmpass;
    do {
      var rootpass = System.console().readPassword(
          "Enter initial password for " + rootUser + getInitialPasswordWarning(initConfig));
      if (rootpass == null) {
        System.exit(0);
      }
      var confirmpass =
          System.console().readPassword("Confirm initial password for " + rootUser + ":");
      if (confirmpass == null) {
        System.exit(0);
      }
      strrootpass = new String(rootpass);
      strconfirmpass = new String(confirmpass);
      if (!strrootpass.equals(strconfirmpass)) {
        log.error("Passwords do not match");
      }
    } while (!strrootpass.equals(strconfirmpass));
    return strrootpass.getBytes(UTF_8);
  }

  /**
   * Create warning message related to initial password, if appropriate.
   * <p>
   * ACCUMULO-2907 Remove unnecessary security warning from console message unless its actually
   * appropriate. The warning message should only be displayed when the value of
   * <code>instance.security.authenticator</code> differs between the SiteConfiguration and the
   * DefaultConfiguration values.
   *
   * @return String containing warning portion of console message.
   */
  private String getInitialPasswordWarning(InitialConfiguration initConfig) {
    String optionalWarning;
    Property authenticatorProperty = Property.INSTANCE_SECURITY_AUTHENTICATOR;
    if (initConfig.get(authenticatorProperty).equals(authenticatorProperty.getDefaultValue())) {
      optionalWarning = ": ";
    } else {
      optionalWarning = " (this may not be applicable for your security setup): ";
    }
    return optionalWarning;
  }

  private static void initSecurity(ServerContext context, Opts opts, String rootUser)
      throws AccumuloSecurityException {
    context.getSecurityOperation().initializeSecurity(context.rpcCreds(), rootUser, opts.rootpass);
  }

  static boolean isInitialized(VolumeManager fs, InitialConfiguration initConfig)
      throws IOException {
    for (String baseDir : initConfig.getVolumeUris()) {
      if (fs.exists(new Path(baseDir, Constants.INSTANCE_ID_DIR))
          || fs.exists(new Path(baseDir, Constants.VERSION_DIR))) {
        return true;
      }
    }
    return false;
  }

  private static boolean addVolumes(VolumeManager fs, InitialConfiguration initConfig,
      ServerDirs serverDirs) {
    var hadoopConf = initConfig.getHadoopConf();
    var siteConfig = initConfig.getSiteConf();
    Set<String> volumeURIs = VolumeConfiguration.getVolumeUris(siteConfig);

    Set<String> initializedDirs = serverDirs.checkBaseUris(hadoopConf, volumeURIs, true);

    HashSet<String> uinitializedDirs = new HashSet<>(volumeURIs);
    uinitializedDirs.removeAll(initializedDirs);

    Path aBasePath = new Path(initializedDirs.iterator().next());
    Path iidPath = new Path(aBasePath, Constants.INSTANCE_ID_DIR);
    Path versionPath = new Path(aBasePath, Constants.VERSION_DIR);

    InstanceId instanceId = VolumeManager.getInstanceIDFromHdfs(iidPath, hadoopConf);
    for (Pair<Path,Path> replacementVolume : serverDirs.getVolumeReplacements()) {
      if (aBasePath.equals(replacementVolume.getFirst())) {
        log.error(
            "{} is set to be replaced in {} and should not appear in {}."
                + " It is highly recommended that this property be removed as data"
                + " could still be written to this volume.",
            aBasePath, Property.INSTANCE_VOLUMES_REPLACEMENTS, Property.INSTANCE_VOLUMES);
      }
    }

    try {
      int persistentVersion = serverDirs
          .getAccumuloPersistentVersion(versionPath.getFileSystem(hadoopConf), versionPath);
      if (persistentVersion != AccumuloDataVersion.get()) {
        throw new IOException("Accumulo " + Constants.VERSION + " cannot initialize data version "
            + persistentVersion);
      }
    } catch (IOException e) {
      log.error("Problem getting accumulo data version", e);
      return false;
    }
    return createDirs(fs, instanceId, uinitializedDirs);
  }

  private static class Opts extends Help {
    @Parameter(names = "--add-volumes",
        description = "Initialize any uninitialized volumes listed in instance.volumes")
    boolean addVolumes = false;
    @Parameter(names = "--reset-security",
        description = "just update the security information, will prompt")
    boolean resetSecurity = false;
    @Parameter(names = {"-f", "--force"},
        description = "force reset of the security information without prompting")
    boolean forceResetSecurity = false;
    @Parameter(names = "--clear-instance-name",
        description = "delete any existing instance name without prompting")
    boolean clearInstanceName = false;
    @Parameter(names = "--upload-accumulo-props",
        description = "Uploads properties in accumulo.properties to Zookeeper")
    boolean uploadAccumuloProps = false;
    @Parameter(names = "--instance-name",
        description = "the instance name, if not provided, will prompt")
    String cliInstanceName = null;
    @Parameter(names = "--password", description = "set the password on the command line")
    String cliPassword = null;
    @Parameter(names = {"-u", "--user"},
        description = "the name of the user to grant system permissions to")
    String rootUser = null;

    byte[] rootpass = null;
  }

  @Override
  public String keyword() {
    return "init";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.CORE;
  }

  @Override
  public String description() {
    return "Initializes Accumulo";
  }

  @Override
  public void execute(final String[] args) {
    boolean success = true;
    Opts opts = new Opts();
    opts.parseArgs("accumulo init", args);
    var siteConfig = SiteConfiguration.auto();
    ZooReaderWriter zoo = new ZooReaderWriter(siteConfig);
    SecurityUtil.serverLogin(siteConfig);
    Configuration hadoopConfig = new Configuration();
    InitialConfiguration initConfig = new InitialConfiguration(hadoopConfig, siteConfig);
    ServerDirs serverDirs = new ServerDirs(siteConfig, hadoopConfig);

    try (var fs = VolumeManagerImpl.get(siteConfig, hadoopConfig)) {
      if (opts.resetSecurity) {
        success = resetSecurity(initConfig, opts, fs);
      }
      if (success && opts.addVolumes) {
        success = addVolumes(fs, initConfig, serverDirs);
      }
      if (!opts.resetSecurity && !opts.addVolumes) {
        success = doInit(zoo, opts, fs, initConfig);
      }
    } catch (IOException e) {
      log.error("Problem trying to get Volume configuration", e);
      success = false;
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
      if (!success) {
        System.exit(-1);
      }
    }
  }

  private boolean resetSecurity(InitialConfiguration initConfig, Opts opts, VolumeManager fs) {
    log.info("Resetting security on accumulo.");
    try (ServerContext context = new ServerContext(initConfig.getSiteConf())) {
      if (!isInitialized(fs, initConfig)) {
        throw new IllegalStateException(
            "FATAL: Attempted to reset security on accumulo before it was initialized");
      }
      if (!opts.forceResetSecurity) {
        String userEnteredName = System.console().readLine("WARNING: This will remove all"
            + " users from Accumulo! If you wish to proceed enter the instance name: ");
        if (userEnteredName != null && !context.getInstanceName().equals(userEnteredName)) {
          throw new IllegalStateException(
              "Aborted reset security: Instance name did not match current instance.");
        }
      }

      final String rootUser = getRootUserName(initConfig, opts);
      opts.rootpass = getRootPassword(initConfig, opts, rootUser);
      initSecurity(context, opts, rootUser);
      return true;
    } catch (Exception e) {
      log.error("Problem calling reset security", e);
      return false;
    }
  }

  public static void main(String[] args) {
    new Initialize().execute(args);
  }
}
