package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingGoalColumn;
import org.apache.accumulo.core.tablet.thrift.THostingGoal;
import org.apache.accumulo.core.util.ColumnFQ;

public enum TabletHostingGoal {

  ALWAYS, DEFAULT, NEVER, ONDEMAND;
  
  public static TabletHostingGoal fromThrift(THostingGoal goal) {
    switch (goal) {
      case ALWAYS:
        return ALWAYS;
      case NEVER:
        return NEVER;
      case ONDEMAND:
        return ONDEMAND;
      default:
        return DEFAULT;
    }
  }
  
  public static TabletHostingGoal fromColumn(String cq) {
    switch (cq) {
      case HostingGoalColumn.ALWAYS:
        return ALWAYS;
      case HostingGoalColumn.NEVER:
        return NEVER;
      case HostingGoalColumn.ONDEMAND:
        return ONDEMAND;
      default:
        return DEFAULT;
    }
  }
  
  public THostingGoal toThrift() {
    switch (this) {
      case ALWAYS:
        return THostingGoal.ALWAYS;
      case NEVER:
        return THostingGoal.NEVER;
      case ONDEMAND:
        return THostingGoal.ONDEMAND;
      default:
        return THostingGoal.DEFAULT;
      
    }
  }

  
  public ColumnFQ getColumn() {
    switch(this) {
      case ALWAYS:
        return HostingGoalColumn.ALWAYS_COLUMN;
      case NEVER:
        return HostingGoalColumn.NEVER_COLUMN;
      case ONDEMAND:
        return HostingGoalColumn.ONDEMAND_COLUMN;
      default:
        return HostingGoalColumn.DEFAULT_COLUMN;
    }
  }

}
