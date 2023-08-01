package org.apache.accumulo.core.tasks;

import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class ThriftSerializer extends StdSerializer<TBase<?,?>> {

  private static final long serialVersionUID = 1L;
  private final TSerializer thriftSerializer;
  
  public ThriftSerializer() {
    super(TBase.class, false);
    try {
      thriftSerializer = new TSerializer(new TBinaryProtocol.Factory());
    } catch (TTransportException e) {
      throw new IllegalStateException("Error creating TBinaryProtocol", e);
    }
  }

  @Override
  public void serialize(TBase<?,?> value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    try {
      gen.writeBinary(thriftSerializer.serialize(value));
    } catch (TException e) {
      throw new IOException("Error serializing object: " + value.toString(), e);
    }
  }

}
