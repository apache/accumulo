package org.apache.accumulo.test.vti;

import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.system.VisibilityTransformingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VisibilityTransformingIteratorTest {

    private static final String VTI_TABLE = "vti";
    private static final ColumnVisibility RAW_VIS = new ColumnVisibility("raw");

    private static MiniAccumuloCluster mac;
    private static Connector rootConn;

    @BeforeClass
    public static void setupMAC() throws Exception {
        Path macPath = Files.createTempDirectory("mac");
        System.out.println("MAC running at " + macPath);
        MiniAccumuloConfig macCfg = new MiniAccumuloConfig(macPath.toFile(), "password");
        macCfg.setNumTservers(1);
        mac = new MiniAccumuloCluster(macCfg);
        mac.start();
        rootConn = mac.getConnector("root", "password");
        rootConn.tableOperations().create(VTI_TABLE);
        rootConn.tableOperations().setProperty("vti", Property.TABLE_VTI_CLASS.getKey(),
                HashingIterator.class.getName());
        rootConn.securityOperations().createLocalUser("hash_user", new PasswordToken("hash_user"));
        rootConn.securityOperations().createLocalUser("raw_user", new PasswordToken("raw_user"));
        rootConn.securityOperations().createLocalUser("all_user", new PasswordToken("all_user"));
        rootConn.securityOperations().changeUserAuthorizations("hash_user", new Authorizations("hash"));
        rootConn.securityOperations().changeUserAuthorizations("raw_user", new Authorizations("raw"));
        rootConn.securityOperations().changeUserAuthorizations("all_user", new Authorizations("hash", "raw"));
    }

    @Test
    public void testVti() throws Exception {
        BatchWriter bw = rootConn.createBatchWriter(VTI_TABLE, new BatchWriterConfig());
        Mutation m = new Mutation("r0");
        m.put("cf0", "cq0", RAW_VIS, new Value("some bytes".getBytes()));
        bw.addMutation(m);
        bw.flush();

        Scanner rawScan = mac.getConnector("all_user", "all_user")
                .createScanner(VTI_TABLE, new Authorizations("hash", "raw"));
        rawScan.setRange(new Range());
        List<Map.Entry<Key,Value>> allKvs = Lists.newArrayList(rawScan);
        assertTrue("All scan must have 2 kv pairs", allKvs.size() == 2);
        assertTrue("First value must be hashed", HashingIterator.isHashedValue(allKvs.get(0).getValue()));
        assertFalse("Second value must not be hashed", HashingIterator.isHashedValue(allKvs.get(1).getValue()));
        assertTrue("Hashed value must have hashed visibility", HashingIterator.isHashedVisibility(allKvs.get(0).getKey()));
        assertTrue("Raw value must have raw visibility", RAW_VIS.equals(allKvs.get(1).getKey().getColumnVisibilityParsed()));
    }

    @Before
    public void clearVtiTable() throws Exception {
        rootConn.tableOperations().deleteRows(VTI_TABLE, null, null);
    }

    @AfterClass
    public static void tearDownMAC() throws Exception {
        mac.stop();
    }

    static class HashingIterator extends VisibilityTransformingIterator {

        private static final String VIS_HASHED = "hashed";

        private static final byte[] HDR = "sha1:".getBytes();
        private static final HashFunction SHA1 = Hashing.sha1();

        private final byte[] out = new byte[HDR.length + SHA1.bits() / 8];

        public HashingIterator() {
            System.arraycopy(HDR, 0, out, 0, HDR.length);
        }

        @Override
        protected Iterable<Map.Entry<ColumnVisibility, Value>> transformVisibility(Key key, Value value) {
            byte[] hash = SHA1.hashBytes(value.get()).asBytes();
            System.arraycopy(hash, 0, out, HDR.length, hash.length);
            return Collections.singletonMap(
                    replaceTerm(key.getColumnVisibilityParsed(), "raw", VIS_HASHED), new Value(out)
            ).entrySet();
        }

        static boolean isHashedValue(Value v) {
            byte[] b = v.get();
            for (int i = 0; i < HDR.length; i++) {
                if (b[i] != HDR[i]) {
                    return false;
                }
            }
            return true;
        }

        static boolean isHashedVisibility(Key k) {
            return VIS_HASHED.equals(k.getColumnVisibility().toString());
        }
    }

}
