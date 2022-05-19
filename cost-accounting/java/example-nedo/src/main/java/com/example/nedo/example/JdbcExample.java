package com.example.nedo.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.example.nedo.BenchConst;

public class JdbcExample {

    public static void main(String[] args) throws SQLException {
        test0();
    }

    static void test0() throws SQLException {
        String url = BenchConst.jdbcUrl();
        String user = BenchConst.jdbcUser();
        String password = BenchConst.jdbcPassword();
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                selectItemMaster(statement);
                insertItemMaster(statement);
                selectItemMaster(statement);
            }
        }
        System.out.println("end");
    }

    static void selectItemMaster(Statement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery("select max(i_id) max_id from item_master")) {
            if (rs != null) {
                while (rs.next()) {
                    int maxId = rs.getInt("max_id");
                    System.out.println("max_id=" + maxId);
                }
            }
        }
    }

    static void insertItemMaster(Statement statement) throws SQLException {
        String sql = "insert into item_master values(1,'2019-04-08','2022-01-16')";
        try {
            int r = statement.executeUpdate(sql);
            System.out.println("insert=" + r);
        } catch (SQLException e) {
            String m = e.getMessage();
            if (m.contains("duplicate key value violates unique constraint")) {
                return;
            }
            throw e;
        }
    }
}
