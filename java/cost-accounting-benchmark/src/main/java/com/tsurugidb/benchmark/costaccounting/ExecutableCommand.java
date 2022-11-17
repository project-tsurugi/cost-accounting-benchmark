package com.tsurugidb.benchmark.costaccounting;

public interface ExecutableCommand {

    public String getDescription();

    public int executeCommand(String... args) throws Exception;
}
