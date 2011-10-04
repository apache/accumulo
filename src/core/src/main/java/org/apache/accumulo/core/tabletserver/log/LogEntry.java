package org.apache.accumulo.core.tabletserver.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;


public class LogEntry {
    public KeyExtent extent;
    public long timestamp;
    public String server;
    public String filename;
    public int tabletId;
    public Collection<String> logSet;
    
    public String toString() {
        return extent.toString() + " " + filename + " (" + tabletId + ")";
    }
    
    public String getName() {
        return server + "/" + filename;
    }
    
    public byte[] toBytes() throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        extent.write(out);
        out.writeLong(timestamp);
        out.writeUTF(server);
        out.writeUTF(filename);
        out.write(tabletId);
        out.write(logSet.size());
        for (String s :logSet) {
            out.writeUTF(s);
        }
        return Arrays.copyOf(out.getData(), out.getLength());
    }
    public void fromBytes(byte bytes[]) throws IOException {
        DataInputBuffer inp = new DataInputBuffer();
        inp.reset(bytes, bytes.length);
        extent = new KeyExtent();
        extent.readFields(inp);
        timestamp = inp.readLong();
        server = inp.readUTF();
        filename = inp.readUTF();
        tabletId = inp.read();
        int count = inp.read();
        ArrayList<String> logSet = new ArrayList<String>(count);
        for (int i = 0; i < count; i++)
            logSet.add(inp.readUTF());
        this.logSet = logSet;
    }
    
}
