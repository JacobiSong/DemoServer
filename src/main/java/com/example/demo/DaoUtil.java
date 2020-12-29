package com.example.demo;

import com.example.demo.datagram.DatagramProto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DaoUtil {
    public static List<DatagramProto.Course> findGroupsByUserId(String userId) {
        List<DatagramProto.Course> list = new ArrayList<DatagramProto.Course>();
        if (userId == null) {
            return list;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            String sql = "select c.id, c.name, c.classroom, c.time, c.semester, c.remarks, c.has_group from course c " +
                    "inner join join_users_with_courses j on c.id = j.course_id where j.user_id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, userId);
            ResultSet rs = ps.executeQuery();
            long hasGroup;
            while (rs.next()) {
                hasGroup = rs.getLong(7);
                if (hasGroup == 0) {
                    continue;
                }
                list.add(buildCourse(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return list;
        }
    }

    private static DatagramProto.Course buildCourse(ResultSet rs) throws SQLException {
        String id;
        String name;
        String classroom;
        String time;
        int semester;
        String remarks;
        DatagramProto.Course.Builder builder = DatagramProto.Course.newBuilder();
        id = rs.getString(1);
        if (id != null) {
            builder.setId(id);
        }
        name = rs.getString(2);
        if (name != null) {
            builder.setName(name);
        }
        classroom = rs.getString(3);
        if (classroom != null) {
            builder.setClassroom(classroom);
        }
        time = rs.getString(4);
        if (time != null) {
            builder.setTime(time);
        }
        semester = rs.getInt(5);
        if (semester != 0) {
            builder.setSemester(semester);
        }
        remarks = rs.getString(6);
        if (remarks != null) {
            builder.setRemarks(remarks);
        }
        return builder.build();
    }

    public static List<DatagramProto.Course> findCoursesByTeacherId(String teacherId) {
        List<DatagramProto.Course> list = new ArrayList<DatagramProto.Course>();
        if (teacherId == null) {
            return list;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            String sql = "select c.id, c.name, c.classroom, c.time, c.semester, c.remarks, c.has_group from course c " +
                    "inner join join_users_with_courses j on c.id = j.course_id where j.user_id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, teacherId);
            ResultSet rs = ps.executeQuery();
            long hasGroup;
            while (rs.next()) {
                hasGroup = rs.getLong(7);
                if (hasGroup != 0) {
                    continue;
                }
                list.add(buildCourse(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return list;
        }
    }

    public static boolean loginCheck(String username, String password) {
        Connection conn = ConnectionUtil.getConn();
        boolean ret = false;
        try {
            String sql = "select password from user where id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, username);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getString(1).equals(password)) {
                    ret = true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean teacherCheck(String id) {
        Connection conn = ConnectionUtil.getConn();
        boolean ret = false;
        try {
            String sql = "select identity from user where id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getInt(1) == 1) {
                    ret = true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return false;
        }
    }

    public static boolean hasUserId(String id) {
        Connection conn = ConnectionUtil.getConn();
        boolean ret = false;
        try {
            String sql = "select id from user where id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                ret = true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean insertUser(DatagramProto.Register register) {
        Connection conn = ConnectionUtil.getConn1();
        String id = register.getUsername();
        String password = register.getPassword();
        int identity = register.getIdentity();
        String sql;
        PreparedStatement ps;
        boolean ret = false;
        try {
            sql = "select identity, name from user where id = ?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && identity == rs.getInt(1)) {
                String name = rs.getString(2);
                ConnectionUtil.closeConn();
                conn = ConnectionUtil.getConn();
                sql = "insert into user (id, password, identity, name, last_modified) values (?, ?, ?, ?, ?)";
                ps = conn.prepareStatement(sql);
                ps.setString(1, id);
                ps.setString(2,password);
                ps.setInt(3, identity);
                ps.setString(4, name);
                ps.setLong(5, System.currentTimeMillis());
                int row = ps.executeUpdate();
                if (row == 1) {
                    ret = true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }
}
