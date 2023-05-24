package com.ellilachen.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Project name: CquirrelDemo
 * Class name：TestJoin
 * Description：Mysql 方法封装
 * Create time：2023/2/5 17:48
 * Creator：ellilachen
 */
public class JDBCUtil {
    private static  final Connection connection = JDBCConnector.getConnection();

    public static void insertIntoTable(String tableName, String[] keys, Object[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append(" (");
        sb.append(String.join(",", keys));
        sb.append(" ) VALUES (");
        sb.append("?,".repeat(keys.length));
        sb.delete(sb.length()-1, sb.length());
        sb.append(")");
        try {
            PreparedStatement ps = connection.prepareStatement(sb.toString());
            int i = 1;
            for (Object value: values) {
                ps.setObject(i, value);
                i += 1;
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void deleteFromTable(String tableName, String[] keys, Object[] values) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tableName).append(" WHERE ");
        List<String> mappedKeys = new ArrayList<>();
        for (String key:keys) {
            mappedKeys.add(key + "=?");
        }
        sb.append(String.join(" AND ", mappedKeys));
        try {
            PreparedStatement ps = connection.prepareStatement(sb.toString());
            int i = 1;
            for (Object value: values) {
                ps.setObject(i, value);
                i += 1;
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void truncateTable(String tableName) {
        try {
            PreparedStatement ps = connection.prepareStatement("truncate " + tableName + ";");
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static ResultSet executeSQL(String queryString) {
        try {
            Statement statement = connection.createStatement();
            return statement.executeQuery(queryString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
