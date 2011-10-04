package org.apache.accumulo.core.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;

public class ByteBufferUtil {
    public static byte[] toBytes(ByteBuffer buffer) {
        if (buffer == null)
            return null;
        return Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.remaining());
    }
    
    public static List<ByteBuffer> toByteBuffers(Collection<byte[]> bytesList) {
        if (bytesList == null)
            return null;
        ArrayList<ByteBuffer> result = new ArrayList<ByteBuffer>();
        for (byte[] bytes : bytesList) {
            result.add(ByteBuffer.wrap(bytes));
        }
        return result;
    }

    public static List<byte[]> toBytesList(Collection<ByteBuffer> bytesList) {
        if (bytesList == null)
            return null;
        ArrayList<byte[]> result = new ArrayList<byte[]>();
        for (ByteBuffer bytes : bytesList) {
            result.add(toBytes(bytes));
        }
        return result;
    }

    public static Text toText(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        Text result = new Text();
        result.set(bytes.array(), bytes.position(), bytes.remaining());
        return result;
    }
    
    public static String toString(ByteBuffer bytes) {
        return new String(bytes.array(), bytes.position(), bytes.remaining());
    }
}
