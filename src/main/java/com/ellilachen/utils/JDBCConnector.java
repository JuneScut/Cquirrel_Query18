package com.ellilachen.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Project name: CquirrelDemo
 * Class name：JDBCConnector
 * Description：TODO
 * Create time：2023/2/5 21:56
 * Creator：ellilachen
 */
public class JDBCConnector {
    static private final String JDBC_URL = "jdbc:mysql://localhost:3306/tpch_test?useSSL=false&characterEncoding=utf8";
    static private final String JDBC_USER = "root";
    static private final String JDBC_PASSWORD = "Ellila@sql123";

    static private Connection connection = null;

    static {
        try {
            connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static public Connection getConnection() {
        return connection;
    }
}
