package org.apache.accumulo.core.metadata.schema;

import org.apache.accumulo.core.dataImpl.KeyExtent;

/**
 * Accumulo Metadata Persistence Layer
 */
public interface Ample {

  TabletMetadata getTablet(KeyExtent extent);



}
