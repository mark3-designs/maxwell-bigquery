package com.steckytech.maxwell.cdc;

public enum DDL {

    table_create("table-create"),
    table_alter("table-alter"),
    table_drop("table-drop"),
    database_create("database-create"),
    database_drop("database-drop")
    ;

    public final String name;

    DDL(String type) {
        this.name = type;
    }

}
