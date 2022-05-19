package com.example.nedo.ddl;

import java.io.BufferedWriter;

import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.ddl.DdlGenarator;
import com.example.nedo.ddl.ddl.OracleTableDdlWriter;
import com.example.nedo.ddl.ddl.TableDdlWriter;

public class OracleDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new OracleDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-oracle.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new OracleTableDdlWriter(table, writer);
    }
}
