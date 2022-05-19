package com.example.nedo.ddl;

import java.io.BufferedWriter;

import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.ddl.DdlGenarator;
import com.example.nedo.ddl.ddl.PostgresqlTableDdlWriter;
import com.example.nedo.ddl.ddl.TableDdlWriter;

public class PostgresqlDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new PostgresqlDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-postgresql.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new PostgresqlTableDdlWriter(table, writer);
    }
}
