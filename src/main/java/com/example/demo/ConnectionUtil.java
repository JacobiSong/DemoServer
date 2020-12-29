package com.example.demo;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionUtil {
    private static String driver;
    private static String url;
    private static String url1;
    private static String username;
    private static String password;
    private static Properties pros = new Properties();
    private static ThreadLocal<Connection> tl = new ThreadLocal<>();

    static {
        InputStream in = ConnectionUtil.class.getClassLoader().getResourceAsStream("db.properties");
        try {
            pros.load(in);
            driver = pros.getProperty("jdbc.driver");
            url = pros.getProperty("jdbc.url");
            url1 = pros.getProperty("jdbc.url1");
            username = pros.getProperty("jdbc.username");
            password = pros.getProperty("jdbc.password");
            Class.forName(driver);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn() {
        Connection conn = tl.get();
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(url, username, password);
                tl.set(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    public static void closeConn() {
        Connection conn = tl.get();
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        tl.set(null);
    }

    public static Connection getConn1() {
        Connection conn = tl.get();
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(url1, username, password);
                tl.set(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }
}
