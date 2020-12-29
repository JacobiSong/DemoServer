package com.example.demo.utils;

import com.example.demo.datagram.DatagramProto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DaoUtil {

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
                String id;
                String name;
                String classroom;
                String time;
                int semester;
                String remarks;
                DatagramProto.Course.Builder builder = DatagramProto.Course.newBuilder();
                id = rs.getString(1);
                name = rs.getString(2);
                classroom = rs.getString(3);
                time = rs.getString(4);
                semester = rs.getInt(5);
                remarks = rs.getString(6);
                builder = builder.setId(id).setName(name).setClassroom(classroom).setTime(time).setSemester(semester);
                if (remarks != null) {
                    builder = builder.setRemarks(remarks);
                }
                list.add(builder.build());
            }
            ps.close();
            rs.close();
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
            ps.close();
            rs.close();
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
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return false;
        }
    }

    public static boolean studentCheck(String id) {
        Connection conn = ConnectionUtil.getConn();
        boolean ret = false;
        try {
            String sql = "select identity from user where id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getInt(1) == 0) {
                    ret = true;
                }
            }
            ps.close();
            rs.close();
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
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    private static DatagramProto.User findUserById (String id) {
        Connection conn = ConnectionUtil.getConn();
        String sql = "select name, identity, phone, email, gender, last_modified from user where id = ?";
        DatagramProto.User.Builder builder = DatagramProto.User.newBuilder();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                int identity = rs.getInt(2);
                String phone = rs.getString(3);
                String email = rs.getString(4);
                int gender = rs.getInt(5);
                long last_modified = rs.getLong(6);
                builder = builder.setId(id).setName(name).setLastModified(last_modified);
                switch (identity) {
                    case 0: {
                        builder = builder.setType(DatagramProto.User.UserType.STUDENT);
                        sql = "select class_no, major, department from student where id = ?";
                        ps.close();
                        ps = conn.prepareStatement(sql);
                        ps.setString(1, id);
                        rs.close();
                        rs = ps.executeQuery();
                        String class_no = rs.getString(1);
                        String major = rs.getString(2);
                        String department = rs.getString(3);
                        DatagramProto.Student.Builder stuBuilder = DatagramProto.Student.newBuilder();
                        if (class_no != null) {
                            stuBuilder = stuBuilder.setClassNo(class_no);
                        }
                        if (major != null) {
                            stuBuilder = stuBuilder.setMajor(major);
                        }
                        if (department != null) {
                            stuBuilder = stuBuilder.setDepartment(department);
                        }
                        builder = builder.setStudent(stuBuilder.build());
                        break;
                    }
                    case 1: {
                        builder = builder.setType(DatagramProto.User.UserType.TEACHER);
                        sql = "select department from teacher where id = ?";
                        ps.close();
                        ps = conn.prepareStatement(sql);
                        ps.setString(1, id);
                        rs.close();
                        rs = ps.executeQuery();
                        String department = rs.getString(1);
                        DatagramProto.Teacher.Builder tchBuilder = DatagramProto.Teacher.newBuilder();
                        if (department != null) {
                            tchBuilder = tchBuilder.setDepartment(department);
                        }
                        builder = builder.setTeacher(tchBuilder.build());
                        break;
                    }
                    default:
                        break;
                }
                switch (gender) {
                    case 0:
                        builder = builder.setGender(DatagramProto.User.Gender.SECRETE);
                        break;
                    case 1:
                        builder = builder.setGender(DatagramProto.User.Gender.FEMALE);
                        break;
                    case 2:
                        builder = builder.setGender(DatagramProto.User.Gender.MALE);
                        break;
                    default:
                        break;
                }
                if (phone != null) {
                    builder = builder.setPhone(phone);
                }
                if (email != null) {
                    builder = builder.setEmail(email);
                }
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return builder.build();
        }
    }

    public static DatagramProto.User findUserById(String id, long time) {
        DatagramProto.User user = null;
        Connection conn = ConnectionUtil.getConn();
        String sql= "select last_modified from user where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getLong(1) == time) {
                    user = DatagramProto.User.newBuilder().build();
                } else {
                    ConnectionUtil.closeConn();
                    user = findUserById(id);
                }
            }
            ps.close();
            rs.close();
        } finally {
            ConnectionUtil.closeConn();
            return user;
        }
    }

    public static boolean insertUser(DatagramProto.Register register) { // TODO : 需要修改
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
                if (ps.executeUpdate() == 1) {
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

    public static boolean updatePhoneById(String id, String phone) {
        if (phone == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update user set phone = ?, last_modified = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, phone);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
            }
            ps.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateEmailById(String id, String email) {
        if (email == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update user set email = ?, last_modified = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, email);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
            }
            ps.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateGenderById(String id, DatagramProto.User.Gender gender) {
        if (gender == null) {
            return false;
        }
        int g = 0;
        switch (gender) {
            case FEMALE:
                g = 1;
                break;
            case MALE:
                g = 2;
                break;
            default:
                break;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update user set gender = ? last_modified = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, g);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
            }
            ps.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updatePasswordById(String id, String password) {
        if (password == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update user set password = ? last_modified = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, password);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
            }
            ps.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateDepartmentByStudentId(String id, String department) {
        if (department == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update student set department = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, department);
            ps.setString(2, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
                ps.close();
                sql = "update user set last_modified = ? where id = ?";
                ps = conn.prepareStatement(sql);
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, id);
                ps.executeQuery();
                ps.close();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateDepartmentByTeacherId(String id, String department) {
        if (department == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update teacher set department = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, department);
            ps.setString(2, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
                ps.close();
                sql = "update user set last_modified = ? where id = ?";
                ps = conn.prepareStatement(sql);
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, id);
                ps.executeQuery();
                ps.close();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateMajorById(String id, String major) {
        if (major == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update student set major = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, major);
            ps.setString(2, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
                ps.close();
                sql = "update user set last_modified = ? where id = ?";
                ps = conn.prepareStatement(sql);
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, id);
                ps.executeQuery();
                ps.close();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }

    public static boolean updateClassNoById(String id, String classNo) {
        if (classNo == null) {
            return false;
        }
        boolean ret = false;
        Connection conn = ConnectionUtil.getConn();
        String sql = "update student set class_no = ? where id = ?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, classNo);
            ps.setString(2, id);
            if (ps.executeUpdate() == 1) {
                ret = true;
                ps.close();
                sql = "update user set last_modified = ? where id = ?";
                ps = conn.prepareStatement(sql);
                ps.setLong(1, System.currentTimeMillis());
                ps.setString(2, id);
                ps.executeQuery();
                ps.close();
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            return ret;
        }
    }
}
