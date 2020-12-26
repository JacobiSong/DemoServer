package com.example.demo;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionUtil {
    private static String driver = "com.mysql.cj.jdbc.Driver";
    private static String url = "jdbc:mysql://221.212.251.156:9888/my_course?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static String username = "dbuser";
    private static String password = "Wenjun@123.hit";
    //private static Properties pros = new Properties();
    private static ThreadLocal<Connection> tl = new ThreadLocal<>();

    /*
    static {
        InputStream in=ConnectionUtil.class.getClassLoader().getResourceAsStream("db.properties");
        try {
            pros.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        driver = pros.getProperty("jdbc.driver");
        url = pros.getProperty("jdbc.url");
        username = pros.getProperty("jdbc.username");
        password = pros.getProperty("jdbc.password");
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    */
    static {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static Connection getConn() throws SQLException {
        Connection conn = tl.get();
        if (conn==null) {
            conn = DriverManager.getConnection(url, username, password);
            tl.set(conn);
        }
        return conn;
    }

    public static void closeConn() throws SQLException {
        Connection conn=tl.get();
        if(conn!=null){
            conn.close();
        }
        tl.set(null);
    }
}
