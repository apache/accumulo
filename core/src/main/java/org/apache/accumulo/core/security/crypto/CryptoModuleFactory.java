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
package org.apache.accumulo.core.security.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.log4j.Logger;

/**
 * This factory module exists to assist other classes in loading crypto modules.
 * 
 * @deprecated This feature is experimental and may go away in future versions.
 */
@Deprecated
public class CryptoModuleFactory {
  
  private static Logger log = Logger.getLogger(CryptoModuleFactory.class);
  
  /**
   * This method returns a crypto module based on settings in the given configuration parameter.
   * 
   * @param conf
   * @return a class implementing the CryptoModule interface. It will *never* return null; rather, it will return a class which obeys the interface but makes no
   *         changes to the underlying data.
   */
  public static CryptoModule getCryptoModule(AccumuloConfiguration conf) {
    String cryptoModuleClassname = conf.get(Property.CRYPTO_MODULE_CLASS);
    return getCryptoModule(cryptoModuleClassname);
  }
  
  @SuppressWarnings({"rawtypes"})
  public static CryptoModule getCryptoModule(String cryptoModuleClassname) {
    log.debug(String.format("About to instantiate crypto module %s", cryptoModuleClassname));
    
    if (cryptoModuleClassname.equals("NullCryptoModule")) {
      return new NullCryptoModule();
    }
    
    CryptoModule cryptoModule = null;
    Class cryptoModuleClazz = null;
    try {
      cryptoModuleClazz = AccumuloVFSClassLoader.loadClass(cryptoModuleClassname);
    } catch (ClassNotFoundException e1) {
      log.warn(String.format("Could not find configured crypto module \"%s\".  NO ENCRYPTION WILL BE USED.", cryptoModuleClassname));
      return new NullCryptoModule();
    }
    
    // Check if the given class implements the CryptoModule interface
    Class[] interfaces = cryptoModuleClazz.getInterfaces();
    boolean implementsCryptoModule = false;
    
    for (Class clazz : interfaces) {
      if (clazz.equals(CryptoModule.class)) {
        implementsCryptoModule = true;
        break;
      }
    }
    
    if (!implementsCryptoModule) {
      log.warn("Configured Accumulo crypto module \"%s\" does not implement the CryptoModule interface. NO ENCRYPTION WILL BE USED.");
      return new NullCryptoModule();
    } else {
      try {
        cryptoModule = (CryptoModule) cryptoModuleClazz.newInstance();
        
        log.debug("Successfully instantiated crypto module");
        
      } catch (InstantiationException e) {
        log.warn(String.format("Got instantiation exception %s when instantiating crypto module \"%s\".  NO ENCRYPTION WILL BE USED.", e.getCause().getClass()
            .getCanonicalName(), cryptoModuleClassname));
        log.warn(e.getCause());
        return new NullCryptoModule();
      } catch (IllegalAccessException e) {
        log.warn(String.format("Got illegal access exception when trying to instantiate crypto module \"%s\".  NO ENCRYPTION WILL BE USED.",
            cryptoModuleClassname));
        log.warn(e);
        return new NullCryptoModule();
      }
    }
    return cryptoModule;
  }
  
  public static SecretKeyEncryptionStrategy getSecretKeyEncryptionStrategy(AccumuloConfiguration conf) {
    String className = conf.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS);
    return getSecretKeyEncryptionStrategy(className);
  }
  
  @SuppressWarnings("rawtypes")
  public static SecretKeyEncryptionStrategy getSecretKeyEncryptionStrategy(String className) {
    if (className == null || className.equals("NullSecretKeyEncryptionStrategy")) {
      return new NullSecretKeyEncryptionStrategy();
    }
    
    SecretKeyEncryptionStrategy strategy = null;
    Class keyEncryptionStrategyClazz = null;
    try {
      keyEncryptionStrategyClazz = AccumuloVFSClassLoader.loadClass(className);
    } catch (ClassNotFoundException e1) {
      log.warn(String.format("Could not find configured secret key encryption strategy \"%s\".  NO ENCRYPTION WILL BE USED.", className));
      return new NullSecretKeyEncryptionStrategy();
    }
    
    // Check if the given class implements the CryptoModule interface
    Class[] interfaces = keyEncryptionStrategyClazz.getInterfaces();
    boolean implementsSecretKeyStrategy = false;
    
    for (Class clazz : interfaces) {
      if (clazz.equals(SecretKeyEncryptionStrategy.class)) {
        implementsSecretKeyStrategy = true;
        break;
      }
    }
    
    if (!implementsSecretKeyStrategy) {
      log.warn("Configured Accumulo secret key encryption strategy \"%s\" does not implement the SecretKeyEncryptionStrategy interface. NO ENCRYPTION WILL BE USED.");
      return new NullSecretKeyEncryptionStrategy();
    } else {
      try {
        strategy = (SecretKeyEncryptionStrategy) keyEncryptionStrategyClazz.newInstance();
        
        log.debug("Successfully instantiated secret key encryption strategy");
        
      } catch (InstantiationException e) {
        log.warn(String.format("Got instantiation exception %s when instantiating secret key encryption strategy \"%s\".  NO ENCRYPTION WILL BE USED.", e
            .getCause().getClass().getCanonicalName(), className));
        log.warn(e.getCause());
        return new NullSecretKeyEncryptionStrategy();
      } catch (IllegalAccessException e) {
        log.warn(String.format("Got illegal access exception when trying to instantiate secret key encryption strategy \"%s\".  NO ENCRYPTION WILL BE USED.",
            className));
        log.warn(e);
        return new NullSecretKeyEncryptionStrategy();
      }
    }
    
    return strategy;
  }
  
  private static class NullSecretKeyEncryptionStrategy implements SecretKeyEncryptionStrategy {
    
    @Override
    public SecretKeyEncryptionStrategyContext encryptSecretKey(SecretKeyEncryptionStrategyContext context) {
      context.setEncryptedSecretKey(context.getPlaintextSecretKey());
      context.setOpaqueKeyEncryptionKeyID("");
      
      return context;
    }
    
    @Override
    public SecretKeyEncryptionStrategyContext decryptSecretKey(SecretKeyEncryptionStrategyContext context) {
      context.setPlaintextSecretKey(context.getEncryptedSecretKey());
      
      return context;
    }
    
    @Override
    public SecretKeyEncryptionStrategyContext getNewContext() {
      return new SecretKeyEncryptionStrategyContext() {
        
        @Override
        public byte[] getPlaintextSecretKey() {
          return plaintextSecretKey;
        }
        
        @Override
        public void setPlaintextSecretKey(byte[] plaintextSecretKey) {
          this.plaintextSecretKey = plaintextSecretKey;
        }
        
        @Override
        public byte[] getEncryptedSecretKey() {
          return encryptedSecretKey;
        }
        
        @Override
        public void setEncryptedSecretKey(byte[] encryptedSecretKey) {
          this.encryptedSecretKey = encryptedSecretKey;
        }
        
        @Override
        public String getOpaqueKeyEncryptionKeyID() {
          return opaqueKeyEncryptionKeyID;
        }
        
        @Override
        public void setOpaqueKeyEncryptionKeyID(String opaqueKeyEncryptionKeyID) {
          this.opaqueKeyEncryptionKeyID = opaqueKeyEncryptionKeyID;
        }
        
        @Override
        public Map<String,String> getContext() {
          return context;
        }
        
        @Override
        public void setContext(Map<String,String> context) {
          this.context = context;
        }
        
        private byte[] plaintextSecretKey;
        private byte[] encryptedSecretKey;
        private String opaqueKeyEncryptionKeyID;
        private Map<String,String> context;
      };
    }
    
  }
  
  private static class NullCryptoModule implements CryptoModule {
    
    @Override
    public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> cryptoOpts) throws IOException {
      return out;
    }
    
    @Override
    public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts) throws IOException {
      return in;
    }
    
    @Override
    public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> conf, Map<CryptoInitProperty,Object> cryptoInitParams) {
      return out;
    }
    
    @Override
    public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts, Map<CryptoInitProperty,Object> cryptoInitParams)
        throws IOException {
      return in;
    }
    
  }
  
}
