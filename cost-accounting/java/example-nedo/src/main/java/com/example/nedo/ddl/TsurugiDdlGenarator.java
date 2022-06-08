package com.example.nedo.ddl;

import java.io.BufferedWriter;

import com.example.nedo.ddl.common.TableSheet;
import com.example.nedo.ddl.ddl.DdlGenarator;
import com.example.nedo.ddl.ddl.TableDdlWriter;
import com.example.nedo.ddl.ddl.TsurugiTableDdlWriter;

public class TsurugiDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new TsurugiDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-tsurugi.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new TsurugiTableDdlWriter(table, writer);
    }
}
