package org.apache.accumulo.core.security.tokens;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

public class TokenHelper {
  private static Logger log = Logger.getLogger(TokenHelper.class);
  
  public static ByteBuffer wrapper(SecurityToken token) throws AccumuloSecurityException {
    return ByteBuffer.wrap(getBytes(token));
  }
  
  // Cannot get typing right to get both warnings resolved. Open to suggestions.
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static byte[] getBytes(SecurityToken token) throws AccumuloSecurityException {
    ByteArrayOutputStream bout = null;
    DataOutputStream out = null;
    try {
      bout = new ByteArrayOutputStream();
      out = new DataOutputStream(bout);
      SecuritySerDe serDe = token.getSerDe();
      WritableUtils.writeCompressedString(out, serDe.getClass().getCanonicalName());
      WritableUtils.writeCompressedByteArray(out, serDe.serialize(token));
      return bout.toByteArray();
    } catch (IOException e) {
      // This shouldn't happen
      throw new RuntimeException(e);
    } finally {
      try {
        if (bout != null)
          bout.close();
        if (out != null)
          out.close();
      } catch (IOException e) {
        log.error(e);
      }
    }
    
  }
  
  public static String asBase64String(SecurityToken token2) throws AccumuloSecurityException {
    return new String(Base64.encodeBase64(getBytes(token2)));
  }
  
  public static SecurityToken fromBase64String(String token) throws AccumuloSecurityException {
    return fromBytes(Base64.decodeBase64(token.getBytes()));
  }
  
  private static SecurityToken fromBytes(byte[] token) throws AccumuloSecurityException {
    String clazz = "";
    ByteArrayInputStream bin = null;
    DataInputStream in = null;
    try {
      try {
        bin = new ByteArrayInputStream(token);
        in = new DataInputStream(bin);
        
        clazz = WritableUtils.readCompressedString(in);
        SecuritySerDe<?> serDe = (SecuritySerDe<?>) Class.forName(clazz).newInstance();
        return serDe.deserialize(WritableUtils.readCompressedByteArray(in));
      } catch (IOException e) {
        // This shouldn't happen
        log.error(e);
        throw new AccumuloSecurityException("unknown user", SecurityErrorCode.INVALID_TOKEN);
      } catch (InstantiationException e) {
        // This shouldn't happen
        log.error(e);
        throw new AccumuloSecurityException("unknown user", SecurityErrorCode.INVALID_TOKEN);
      } catch (IllegalAccessException e) {
        // This shouldn't happen
        log.error(e);
        throw new AccumuloSecurityException("unknown user", SecurityErrorCode.INVALID_TOKEN);
      } 
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to load class " + clazz, e);
    } finally {
      try {
        if (bin != null)
          bin.close();
        if (in != null)
          in.close();
      } catch (IOException e) {
        log.error(e);
      }
    }
  }
  
  public static SecurityToken unwrap(ByteBuffer token) throws AccumuloSecurityException {
    return fromBytes(ByteBufferUtil.toBytes(token));
  }
}
