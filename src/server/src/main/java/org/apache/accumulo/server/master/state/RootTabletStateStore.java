package org.apache.accumulo.server.master.state;

import java.util.Iterator;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;


public class RootTabletStateStore extends MetaDataStateStore {
    
    public RootTabletStateStore(CurrentState state) {
        super(state);
    }

    @Override
    public Iterator<TabletLocationState> iterator() {
        Range range = new Range(Constants.ROOT_TABLET_EXTENT.getMetadataEntry(), false, 
                                KeyExtent.getMetadataEntry(new Text(Constants.METADATA_TABLE_ID), null), true);
        return new MetaDataTableScanner(range, state);
    }

    @Override
    public String name() {
        return "Non-Root Metadata Tablets";
    }
}
