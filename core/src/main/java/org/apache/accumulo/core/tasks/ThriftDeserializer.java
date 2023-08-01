package org.apache.accumulo.core.tasks;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class ThriftDeserializer extends StdDeserializer<TBase<?,?>> {

  private static final long serialVersionUID = 1L;
  private final TDeserializer thriftDeserializer;

  public ThriftDeserializer() {
    super(TBase.class);
    try {
      thriftDeserializer = new TDeserializer(new TBinaryProtocol.Factory());
    } catch (TTransportException e) {
      throw new IllegalStateException("Error creating TBinaryProtocol", e);
    }
  }

  @Override
  public TBase<?,?> deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException, JacksonException {
    try {
      @SuppressWarnings("unchecked")
      Class<TBase<?,?>> object = (Class<TBase<?,?>>) this.handledType();
      TBase<?,?> instance = object.getConstructor().newInstance();
      thriftDeserializer.deserialize(instance, p.getBinaryValue());
      return instance;
    } catch (TException | InstantiationException | IllegalAccessException |
        IllegalArgumentException | InvocationTargetException | NoSuchMethodException |
        SecurityException e) {
      throw new IOException("Error deserializing object: " + this.handledType(), e);
    }
  }

}
