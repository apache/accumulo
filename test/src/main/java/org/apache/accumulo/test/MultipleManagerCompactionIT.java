package org.apache.accumulo.test;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.compaction.ExternalCompactionTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;

public class MultipleManagerCompactionIT extends AccumuloClusterHarness {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
        ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
        cfg.getClusterServerConfiguration().setNumManagers(3);
    }

    @Test
    public void test() throws  Exception {
        String[] names = this.getUniqueNames(2);
        try (AccumuloClient client =
                     Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

            String table1 = names[0];
            createTable(client, table1, "cs1");

            String table2 = names[1];
            createTable(client, table2, "cs2");

            writeData(client, table1);
            writeData(client, table2);

            compact(client, table1, 2, GROUP1, true);
            verify(client, table1, 2);

            SortedSet<Text> splits = new TreeSet<>();
            splits.add(new Text(row(MAX_DATA / 2)));
            client.tableOperations().addSplits(table2, splits);

            compact(client, table2, 3, GROUP2, true);
            verify(client, table2, 3);

        }
    }



}
