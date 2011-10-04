package org.apache.accumulo.server.util;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DumpZookeeper {
    
    static ZooKeeper zk = null;
    
    private static final Logger log = Logger.getLogger(DumpZookeeper.class);
    
    private static class Encoded {
        public String encoding;
        public String value;
        Encoded(String e, String v) { encoding = e; value = v; }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.WARN);
        PrintStream out = System.out;
        int timeout = 30 * 1000;
        String server = args[0];
        String root = "/";
        if (args.length > 1)
            root = args[1];
        try {
            zk = new ZooKeeper(server, timeout, new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                }
            });
            write(out, 0, "<dump root='%s'>", root);
            for (String child : zk.getChildren(root, null))
                if (!child.equals("zookeeper"))
                    dump(out, root, child, 1);
            write(out, 0, "</dump>");
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }
    
    private static void dump(PrintStream out, String root, String child, int indent) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String path = root + "/" + child;
        if (root.endsWith("/"))
            path = root + child;
        Stat stat = zk.exists(path, null);
        if (stat == null)
            return;
        String type = "node";
        if (stat.getEphemeralOwner() != 0) {
            type = "ephemeral";
        }
        if (stat.getNumChildren() == 0) {
            if (stat.getDataLength() == 0) {
                write(out, indent, "<%s name='%s'/>", type, child);
            } else {
                Encoded value = value(path);
                write(out, indent, "<%s name='%s' encoding='%s' value='%s'/>", type, child, value.encoding, value.value);
            }
        } else {
            if (stat.getDataLength() == 0) {
                write(out, indent, "<%s name='%s'>", type, child);
            } else {
                Encoded value = value(path);
                write(out, indent, "<%s name='%s' encoding='%s' value='%s'>", type, child, value.encoding, value.value);
            }
            for (String c : zk.getChildren(path, null)) {
                dump(out, path, c, indent + 1);
            }
            write(out, indent, "</node>");
        }
    }

    private static Encoded value(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zk.getData(path, null, null);
        for (int i = 0; i < data.length; i++) {
            // does this look like simple ascii?
            if (data[i] < ' ' || data[i] > '~')
                return new Encoded("base64", new String(Base64.encodeBase64(data), "utf8"));
        }
        return new Encoded("utf8", new String(data, "utf8"));
    }

    private static void write(PrintStream out, int indent, String fmt,
                              Object... args) {
        for (int i = 0; i < indent; i++) 
            out.print(" ");
        out.println(String.format(fmt, args));
    }
}
