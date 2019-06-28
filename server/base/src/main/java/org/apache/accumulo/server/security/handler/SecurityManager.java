package org.apache.accumulo.server.security.handler;

import java.io.IOException;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.security.Policy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.core.spi.security.SecurityModule;

public class SecurityManager {

  ServerContext context;
  SecurityModule module;
  Policy policy;

  public SecurityManager(ServerContext context) {
    this.context = context;
    ServiceEnvironment env = new ServiceEnvironmentImpl(context);
    ServiceEnvironment.Configuration conf = env.getConfiguration();
    this.module = loadModule(conf);
    this.policy = loadPolicy(this.module);
  }

  private SecurityModule loadModule(ServiceEnvironment.Configuration conf) {
    try {
      String securityModuleClass = conf.get(Property.INSTANCE_SECURITY_MODULE.getKey());
      return ConfigurationTypeHelper.getClassInstance(null, securityModuleClass, SecurityModule.class);
    } catch (IOException | ReflectiveOperationException e) {
    throw new RuntimeException(e);
  }
  }

  private Policy loadPolicy(SecurityModule module){
   return module.policy();
  }





}
