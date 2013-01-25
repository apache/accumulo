package org.apache.accumulo.core.security.tokens;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class TokenHelper {
  private static Logger log = Logger.getLogger(TokenHelper.class);
  
  public static ByteBuffer wrapper(AccumuloToken<?,?> token) {
    return ByteBuffer.wrap(getBytes(token));
  }
  
  private static byte[] getBytes(AccumuloToken<?,?> token) {
    TSerializer serializer = new TSerializer();
    ByteArrayOutputStream bout = null;
    DataOutputStream out = null;
    try {
      bout = new ByteArrayOutputStream();
      out = new DataOutputStream(bout);
      WritableUtils.writeCompressedString(out, token.getClass().getCanonicalName());
      
      WritableUtils.writeCompressedByteArray(out, serializer.serialize(token));
      return bout.toByteArray();
    } catch (TException te) {
      // This shouldn't happen
      throw new RuntimeException(te);
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
  
  public static String asBase64String(AccumuloToken<?,?> token2) {
    return new String(Base64.encodeBase64(getBytes(token2)));
  }
  
  public static AccumuloToken<?,?> fromBase64String(String token) {
    return fromBytes(Base64.decodeBase64(token.getBytes()));
  }
  
  private static AccumuloToken<?,?> fromBytes(byte[] token) {
    TDeserializer deserializer = new TDeserializer();
    String clazz = "";
    ByteArrayInputStream bin = null;
    DataInputStream in = null;
    try {
      try {
        bin = new ByteArrayInputStream(token);
        in = new DataInputStream(bin);
        
        clazz = WritableUtils.readCompressedString(in);
        AccumuloToken<?,?> obj = (AccumuloToken<?,?>) Class.forName(clazz).newInstance();
        
        byte[] tokenBytes = WritableUtils.readCompressedByteArray(in);
        deserializer.deserialize(obj, tokenBytes);
        
        return obj;
      } catch (IOException e) {
        // This shouldn't happen
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        // This shouldn't happen
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        // This shouldn't happen
        throw new RuntimeException(e);
      } catch (TException e) {
        // This shouldn't happen
        throw new RuntimeException(e);
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
  
  public static AccumuloToken<?,?> unwrap(ByteBuffer token) {
    return fromBytes(ByteBufferUtil.toBytes(token));
  }
}
