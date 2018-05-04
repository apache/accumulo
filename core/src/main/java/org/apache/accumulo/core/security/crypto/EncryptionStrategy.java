package org.apache.accumulo.core.security.crypto;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

public interface EncryptionStrategy {

  public void encryptStream(OutputStream outputStream);

  public void decryptStream(InputStream inputStream);

  public Map<String,String> properties = new HashMap<>();

  default void readProperties(AccumuloConfiguration conf) {
    this.properties.putAll(conf.getAllPropertiesWithPrefix(Property.CRYPTO_PREFIX));
  }

  default void setProperties(Map<String, String> properties) {
    this.properties.putAll(properties);
  }

  default Map<String,String> getProperties() {
    return properties;
  }
}
