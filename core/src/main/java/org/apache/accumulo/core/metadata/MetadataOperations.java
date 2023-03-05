package org.apache.accumulo.core.metadata;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.hadoop.io.Text;

import java.util.*;

public class MetadataOperations {

    public void compact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, Set<StoredTabletFile> inputFiles, TabletFile outputFile, DataFileValue dfv) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        inputFiles.forEach(conditionalMutator::requireFile);
        //TODO ensure this does not require the encoded version
        conditionalMutator.requirePrevEndRow(extent);
        //TODO could add a conditional check to ensure file is not already there


        inputFiles.forEach(conditionalMutator::deleteFile);
        conditionalMutator.putFile(outputFile, dfv);

        conditionalMutator.submit();
    }

    public void minorCompact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, TServerInstance tsi, TabletFile newFile, DataFileValue dfv){
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        //when a tablet has a current location its state should always be nominal, so this check is a bit redundant
        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.CURRENT);
        conditionalMutator.requirePrevEndRow(extent);
        //TODO could add a conditional check to ensure file is not already there

        conditionalMutator.putFile(newFile, dfv);

        conditionalMutator.submit();
    }

    public void bulkImport(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, Map<TabletFile, DataFileValue> newFiles) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requirePrevEndRow(extent);
        //TODO require files to be absent?

        newFiles.forEach(conditionalMutator::putFile);

        conditionalMutator.submit();
    }

    public void setFuture(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, TServerInstance tsi) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requireAbsentLocation();
        conditionalMutator.requirePrevEndRow(extent);

        conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.FUTURE);

        conditionalMutator.submit();
    }

    public void setCurrent(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, TServerInstance tsi) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.FUTURE)
        conditionalMutator.requirePrevEndRow(extent);

        conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.CURRENT);
        conditionalMutator.deleteLocation(tsi, TabletMetadata.LocationType.FUTURE);

        conditionalMutator.submit();
    }

    public void deleteLocation(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, TServerInstance tsi, TabletMetadata.LocationType locType) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requireLocation(tsi, locType)
        conditionalMutator.requirePrevEndRow(extent);

        conditionalMutator.deleteLocation(tsi, locType);

        conditionalMutator.submit();
    }

    public void addTablet(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, String path, TimeType timeType){
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireAbsentTablet();

        conditionalMutator.putPrevEndRow(extent.prevEndRow());
        conditionalMutator.putDirName(path);
        conditionalMutator.putTime(new MetadataTime(0, timeType));
        //TODO used to add lock entry, that can probably go away

        conditionalMutator.submit();
    }

    public void initiateSplit(Ample.ConditionalTabletsMutator ctm, KeyExtent extent) {
        Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

        conditionalMutator.requireState(Ample.TabletState.NOMINAL);
        conditionalMutator.requireAbsentLocation();
        conditionalMutator.requirePrevEndRow(extent);

        // setting the splitting state should prevent all other operations on the tablet like compact,merge,bulk import,load
        conditionalMutator.setState(Ample.TabletState.SPLITTING);

        conditionalMutator.submit();
    }

    //Tablets used only split into two children.  Now that its a metadata only operation, a single tablet can split into multiple children.
    public boolean doSplit(Ample ample, KeyExtent extent, SortedSet<Text> splits) {

        var tabletsMutator = ample.conditionallyMutateTablets();

        var tabletMutator = tabletsMutator.mutateTablet(extent);

        tabletMutator.requireAbsentLocation();
        tabletMutator.requireState(Ample.TabletState.NOMINAL);
        tabletMutator.requirePrevEndRow(extent);
        tabletMutator.putState(Ample.TabletState.SPLITTING);
        //TODO maybe put something in metadata table that indicate what splits are being done like a hash of the splits?
        tabletMutator.submit();

        var result = tabletsMutator.process().get(extent);
        //do not need to check the result because this method is idempotent and subsequent calls would be expected to fail. Will read the tablet later if this failed and its not in the splitting state will fail then.

        Set<KeyExtent> newExtents = new HashSet<>();

        Text prev = extent.prevEndRow();
        for (var split : splits) {
            Preconditions.checkArgument(extent.contains(split));
            newExtents.add(new KeyExtent(extent.tableId(), split, prev));
            prev = split;
        }

        var lastExtent = new KeyExtent(extent.tableId(), extent.endRow(), prev);
        newExtents.add(lastExtent);

        Map<KeyExtent, TabletMetadata> existingMetadata = new TreeMap<>();

        // this method should be idempotent, so need ascertain where we are at in the split process
        try(var tablets = ample.readTablets().forTable(extent.tableId()).overlapping(extent.prevEndRow(), extent.endRow()).build()){
                tablets.forEach(tabletMetadata -> existingMetadata.put(tabletMetadata.getExtent(), tabletMetadata));
        }
        
        if(existingMetadata.isEmpty()) {
            //TODO use diff exception and provide a good message
            throw new RuntimeException();
        }





    }


}
