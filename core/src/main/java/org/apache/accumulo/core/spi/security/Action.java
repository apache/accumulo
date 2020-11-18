package org.apache.accumulo.core.spi.security;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;

public interface Action {
  String description();

  abstract class UserAction implements Action {

    public static class Create extends UserAction {

      @Override
      public String description() {
        return "Create a new user";
      }
    }
  }

  abstract class TableAction implements Action {
    String tableName;
    TableId id;

    public TableAction(String tableName, TableId id) {
      this.tableName = tableName;
      this.id = id;
    }

    public String getTableName() {
      return tableName;
    }

    public TableId getId() {
      return id;
    }

    public static class BulkImport extends TableAction {

      public BulkImport(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Bulk import to a table";
      }
    }
    public static class CancelCompact extends TableAction {

      public CancelCompact(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Cancel the compaction of a table";
      }
    }
    public static class Clone extends TableAction {

      public Clone(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Clone a table";
      }
    }
    public static class Compact extends TableAction {

      public Compact(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Compact a table";
      }
    }
    public static class ConditionalUpdate extends TableAction {

      public ConditionalUpdate(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Conditionally update a table";
      }
    }
    public static class Create extends TableAction {

      public Create(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Create a new table";
      }
    }
    public static class Drop extends TableAction {

      public Drop(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Drop a table";
      }
    }
    public static class Export extends TableAction {

      public Export(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Export a table";
      }
    }
    public static class Flush extends TableAction {

      public Flush(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Flush a table to disk";
      }
    }
    public static class Import extends TableAction {

      public Import(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Import a table";
      }
    }
    public static class Merge extends TableAction {

      public Merge(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Merge a table";
      }
    }
    public static class Offline extends TableAction {

      public Offline(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Offline a table";
      }
    }
    public static class Online extends TableAction {

      public Online(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Online a table";
      }
    }
    public static class PropertyChange extends TableAction {

      public PropertyChange(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Change a table property";
      }
    }
    public static class RangeDeletion extends TableAction {

      public RangeDeletion(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Delete a range of a table";
      }
    }
    public static class Rename extends TableAction {

      public Rename(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Rename a table";
      }
    }
    public static class Scan extends TableAction {

      public Scan(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Scan a table";
      }
    }
    public static class Split extends TableAction {

      public Split(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Create table splits";
      }
    }
    public static class Summarize extends TableAction {

      public Summarize(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Create summaries on a table";
      }
    }
    public static class Write extends TableAction {

      public Write(String tableName, TableId id) {
        super(tableName, id);
      }

      @Override
      public String description() {
        return "Write to a table";
      }
    }

  }

}
