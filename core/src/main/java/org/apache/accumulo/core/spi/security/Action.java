package org.apache.accumulo.core.spi.security;

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

    TableAction(String tableName) {
      this.tableName = tableName;
    }

    public static class Create extends TableAction {

      Create(String tableName) {
        super(tableName);
      }

      @Override
      public String description() {
        return "Create a new table";
      }
    }
  }

}
