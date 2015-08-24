package org.apache.accumulo.monitor.util.celltypes;

import org.apache.accumulo.core.master.thrift.BulkImportState;

public class BulkImportStateType extends CellType<BulkImportState> {

  private static final long serialVersionUID = 1L;

  @Override
  public String alignment() {
    return "left";
  }

  @Override
  public String format(Object obj) {
    BulkImportState state = (BulkImportState)obj;
    return state.name();
  }

  @Override
  public int compare(BulkImportState o1, BulkImportState o2) {
    if (o1 == null && o2 == null)
      return 0;
    else if (o1 == null)
      return -1;
    return o1.compareTo(o2);
  }

}