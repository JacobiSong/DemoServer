package com.example.demo.utils;

import com.example.demo.datagram.DatagramProto;
import com.example.demo.server.PushItem;
import com.google.protobuf.ByteString;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DaoUtil {

    public static List<DatagramProto.Course> findCoursesByTeacherId(String teacherId) {
        List<DatagramProto.Course> list = new ArrayList<>();
        if (teacherId == null) {
            return list;
        }
        Connection conn = ConnectionUtil.getConn();
        Connection conn1 = ConnectionUtil.getConn1();
        try {
            PreparedStatement ps1 = conn1.prepareStatement("select id, name, classroom, time, semester, remarks " +
                    "from course inner join t_join on id = course_id where user_id = ?");
            ps1.setString(1, teacherId);
            ResultSet rs1 = ps1.executeQuery();
            while (rs1.next()) {
                String id;
                id = rs1.getString(1);
                PreparedStatement ps = conn.prepareStatement("select count(id) from course where id = ?");
                ps.setString(1, id);
                ResultSet rs = ps.executeQuery();
                if (rs.next() && rs.getInt(1) == 0) {
                    String name;
                    String classroom;
                    String time;
                    String semester;
                    String remarks;
                    name = rs1.getString(2);
                    classroom = rs1.getString(3);
                    time = rs1.getString(4);
                    semester = rs1.getString(5);
                    remarks = rs1.getString(6);
                    DatagramProto.Course.Builder builder = DatagramProto.Course.newBuilder().setId(id).setName(name)
                            .setClassroom(classroom).setTime(time).setSemester(semester);
                    if (remarks != null) {
                        builder.setRemarks(remarks);
                    }
                    list.add(builder.build());
                }
                ps.close();
                rs.close();
            }
            ps1.close();
            rs1.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            ConnectionUtil.closeConn1();
        }
        return list;
    }

    public static boolean loginCheck(String username, String password) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select password from user where id = ?");
            ps.setString(1, username);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getString(1).equals(password)) {
                ps.close();
                rs.close();
                return true;
            }
            ps.close();
            rs.close();
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean teacherCheck(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select identity from user where id = ?");
            return findIdInDb(id, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean studentCheck(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select identity from user where id = ?");
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                ps.close();
                rs.close();
                return true;
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
        return false;
    }

    public static boolean hasUserId(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select count(id) from user where id = ?");
            return findIdInDb(id, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    private static boolean findIdInDb(String id, PreparedStatement ps) throws SQLException {
        ps.setString(1, id);
        ResultSet rs = ps.executeQuery();
        if (rs.next() && rs.getInt(1) == 1) {
            ps.close();
            rs.close();
            return true;
        }
        ps.close();
        rs.close();
        return false;
    }

    public static boolean hasGroupId(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select count(id) from course where id = ?");
            return findIdInDb(id, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static DatagramProto.User findUserById(String id) {
        Connection conn = ConnectionUtil.getConn();
        DatagramProto.User.Builder builder = DatagramProto.User.newBuilder();
        try {
            File file = new File("/usr/img" + File.separator + id + "_img.jpg");
            if (file.exists()) {
                int length = (int) file.length();
                byte[] data = new byte[length];
                if (new FileInputStream(file).read(data) > 0) {
                    builder.setPhoto(ByteString.copyFrom(data));
                }
            }
            PreparedStatement ps = conn.prepareStatement("select name, identity, phone, email, gender, last_modified" +
                    " from user where id = ?");
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                int identity = rs.getInt(2);
                String phone = rs.getString(3);
                String email = rs.getString(4);
                int gender = rs.getInt(5);
                long last_modified = rs.getLong(6);
                builder.setId(id).setName(name).setLastModified(last_modified).setIdentityValue(identity);
                ps.close();
                rs.close();
                switch (identity) {
                    case 0: {
                        ps = conn.prepareStatement("select class_no, major, department from student where id = ?");
                        ps.setString(1, id);
                        rs = ps.executeQuery();
                        if (rs.next()) {
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
                            builder.setStudent(stuBuilder.build());
                        }
                        ps.close();
                        rs.close();
                        break;
                    }
                    case 1: {
                        ps = conn.prepareStatement("select department from teacher where id = ?");
                        ps.setString(1, id);
                        rs = ps.executeQuery();
                        if (rs.next()) {
                            String department = rs.getString(1);
                            DatagramProto.Teacher.Builder tchBuilder = DatagramProto.Teacher.newBuilder();
                            if (department != null) {
                                tchBuilder = tchBuilder.setDepartment(department);
                            }
                            builder.setTeacher(tchBuilder.build());
                        }
                        ps.close();
                        rs.close();
                        break;
                    }
                    default:
                        break;
                }
                switch (gender) {
                    case 0:
                        builder.setGender(DatagramProto.User.Gender.SECRETE);
                        break;
                    case 1:
                        builder.setGender(DatagramProto.User.Gender.FEMALE);
                        break;
                    case 2:
                        builder.setGender(DatagramProto.User.Gender.MALE);
                        break;
                    default:
                        break;
                }
                if (phone != null) {
                    builder.setPhone(phone);
                }
                if (email != null) {
                    builder.setEmail(email);
                }
                return builder.build();
            }
            ps.close();
            rs.close();
        } catch (SQLException | FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static DatagramProto.User findUserById(String id, long time) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select last_modified from user where id = ?");
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                if (rs.getLong(1) == time) {
                    return DatagramProto.User.newBuilder().build();
                } else {
                    return findUserById(id);
                }
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static DatagramProto.User insertUser(DatagramProto.Register register) {
        Connection conn = ConnectionUtil.getConn();
        Connection conn1 = ConnectionUtil.getConn1();
        String id = register.getUsername();
        String password = register.getPassword();
        int identity = register.getIdentityValue();
        try {
            PreparedStatement ps1 = conn1.prepareStatement("select identity, name from user where id = ?");
            ps1.setString(1, id);
            ResultSet rs1 = ps1.executeQuery();
            if (rs1.next() && identity == rs1.getInt(1)) {
                String name = rs1.getString(2);
                PreparedStatement ps = conn.prepareStatement(
                        "insert ignore into user (id, name, identity, password, last_modified, create_time) " +
                                "values (?, ?, ?, ?, ?, ?)");
                ps.setString(1, id);
                ps.setString(2, name);
                ps.setInt(3, identity);
                ps.setString(4, password);
                long time = System.currentTimeMillis();
                ps.setLong(5, time);
                ps.setLong(6, time);
                if (ps.executeUpdate() == 1) {
                    ps.close();
                    switch (identity) {
                        case 0:
                            ps = conn.prepareStatement("insert ignore into student (id) values (?)");
                            ps.setString(1, id);
                            ps.executeUpdate();
                            ps.close();
                            break;
                        case 1:
                            ps = conn.prepareStatement("insert ignore into teacher (id) values (?)");
                            ps.setString(1, id);
                            ps.executeUpdate();
                            ps.close();
                            break;
                        default:
                            break;
                    }
                    return DatagramProto.User.newBuilder().setId(id).setName(name).setCreateTime(time).build();
                }
                ps.close();
            }
            ps1.close();
            rs1.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
            ConnectionUtil.closeConn1();
        }
        return null;
    }

    public static boolean updatePhoneById(String id, String phone) {
        if (phone == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update user set phone = ?, last_modified = ? where id = ?");
            return updateUserAttr(id, phone, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean updateEmailById(String id, String email) {
        if (email == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update user set email = ?, last_modified = ? where id = ?");
            return updateUserAttr(id, email, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    private static boolean updateUserAttr(String id, String attr, PreparedStatement ps) throws SQLException {
        ps.setString(1, attr);
        ps.setLong(2, System.currentTimeMillis());
        ps.setString(3, id);
        if (ps.executeUpdate() == 1) {
            ps.close();
            return true;
        }
        ps.close();
        return false;
    }

    public static boolean updateGenderById(String id, DatagramProto.User.Gender gender) {
        if (gender == null) {
            return false;
        }
        int g = gender.getNumber();
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update user set gender = ?, last_modified = ? where id = ?");
            ps.setInt(1, g);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, id);
            if (ps.executeUpdate() == 1) {
                ps.close();
                return true;
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
        return false;
    }

    public static boolean updatePasswordById(String id, String password) {
        if (password == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update user set password = ? where id = ?");
            ps.setString(1, password);
            ps.setString(2, id);
            if (ps.executeUpdate() == 1) {
                ps.close();
                return true;
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
        return false;
    }

    public static boolean updateDepartmentByStudentId(String id, String department) {
        if (department == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update student set department = ? where id = ?");
            return updateStudentOrTeacherAttr(id, department, conn, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    private static boolean updateStudentOrTeacherAttr(String id, String attr, Connection conn, PreparedStatement ps) throws SQLException {
        ps.setString(1, attr);
        ps.setString(2, id);
        if (ps.executeUpdate() == 1) {
            ps.close();
            ps = conn.prepareStatement("update user set last_modified = ? where id = ?");
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, id);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        ps.close();
        return false;
    }

    public static boolean updateDepartmentByTeacherId(String id, String department) {
        if (department == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update teacher set department = ? where id = ?");
            return updateStudentOrTeacherAttr(id, department, conn, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean updateMajorById(String id, String major) {
        if (major == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update student set major = ? where id = ?");
            return updateStudentOrTeacherAttr(id, major, conn, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean updateClassNoById(String id, String classNo) {
        if (classNo == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("update student set class_no = ? where id = ?");
            return updateStudentOrTeacherAttr(id, classNo, conn, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static boolean insertGroup(String userId, String courseId) {
        if (userId == null || courseId == null) {
            return false;
        }
        Connection conn = ConnectionUtil.getConn();
        Connection conn1 = ConnectionUtil.getConn1();
        try {
            PreparedStatement ps1 = conn1.prepareStatement("select count(*) from t_join where user_id = ? " +
                    "and course_id = ?");
            ps1.setString(1, userId);
            ps1.setString(2, courseId);
            ResultSet rs1 = ps1.executeQuery();
            if (rs1.next() && rs1.getInt(1) == 1) {
                ps1.close();
                rs1.close();
                ps1 = conn1.prepareStatement("select user_id from t_join where course_id = ?");
                ps1.setString(1, courseId);
                rs1 = ps1.executeQuery();
                while (rs1.next()) {
                    String uid = rs1.getString(1);
                    PreparedStatement ps = conn.prepareStatement("insert ignore into t_join (user_id, course_id) " +
                            "values (?, ?)");
                    ps.setString(1, uid);
                    ps.setString(2, courseId);
                    ps.executeUpdate();
                    ps.close();
                }
                ps1.close();
                rs1.close();
                ps1 = conn1.prepareStatement("select name, classroom, time, semester, remarks from course where id = ?");
                ps1.setString(1, courseId);
                rs1 = ps1.executeQuery();
                if (rs1.next()) {
                    String name = rs1.getString(1);
                    String classroom = rs1.getString(2);
                    String time = rs1.getString(3);
                    String semester = rs1.getString(4);
                    PreparedStatement ps = conn.prepareStatement("insert ignore into course " +
                            "(id, name, classroom, time, semester, last_modified) values (?, ?, ?, ?, ?, ?)");
                    ps.setString(1, courseId);
                    ps.setString(2, name);
                    ps.setString(3, classroom);
                    ps.setString(4, time);
                    ps.setString(5, semester);
                    ps.setLong(6, System.currentTimeMillis());
                    if (ps.executeUpdate() == 1) {
                        ps.close();
                        String remarks = rs1.getString(5);
                        if (remarks != null) {
                            ps = conn.prepareStatement("update course set remarks = ? where id = ?");
                            ps.setString(1, remarks);
                            ps.setString(2, courseId);
                            ps.executeUpdate();
                            ps.close();
                        }
                        ps = conn.prepareStatement("create table if not exists " + courseId + "_m (" +
                                "id bigint primary key not null auto_increment, sender_id varchar(10) not null, " +
                                "receiver_id varchar(10) not null, content varchar(1000) not null, time bigint not null, " +
                                "temporary_id int, constraint uk1 unique (sender_id, time, temporary_id))");
                        ps.executeUpdate();
                        ps.close();
                        ps = conn.prepareStatement("create table if not exists " + courseId + "_n (" +
                                "id bigint primary key not null auto_increment, sender_id varchar(10) not null, " +
                                "receiver_id varchar(10) not null, title varchar(20) not null, content varchar(1000) not null, " +
                                "time bigint not null, temporary_id int, constraint uk1 unique (sender_id, time, temporary_id))");
                        ps.executeUpdate();
                        ps.close();
                        return true;
                    }
                }
            }
            ps1.close();
            rs1.close();
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            ConnectionUtil.closeConn();
            ConnectionUtil.closeConn1();
        }
    }

    public static DatagramProto.Group findGroupById(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "select name, classroom, time, semester, last_modified, remarks from course where id = ?");
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                DatagramProto.Course.Builder courseBuilder = DatagramProto.Course.newBuilder().setId(id)
                        .setName(rs.getString(1))
                        .setClassroom(rs.getString(2)).setTime(rs.getString(3))
                        .setSemester(rs.getString(4)).setLastModified(rs.getLong(5));
                String remarks = rs.getString(6);
                if (remarks != null) {
                    courseBuilder.setRemarks(remarks);
                }
                DatagramProto.Group.Builder groupBuilder = DatagramProto.Group.newBuilder().setCourse(courseBuilder.build());
                ps.close();
                rs.close();
                ps = conn.prepareStatement(
                        "select user_id, name, identity, create_time from t_join left join user on user_id = id where course_id = ?");
                ps.setString(1, id);
                rs = ps.executeQuery();
                DatagramProto.Users.Builder usersBuilder = DatagramProto.Users.newBuilder();
                while (rs.next()) {
                    String name = rs.getString(2);
                    if (name != null) {
                        usersBuilder.addUsers(DatagramProto.User.newBuilder().setId(rs.getString(1))
                                .setName(name).setIdentityValue(rs.getInt(3)).setCreateTime(rs.getLong(4)).build());
                    } else {
                        usersBuilder.addUsers(DatagramProto.User.newBuilder().setId(rs.getString(1)).build());
                    }
                }
                groupBuilder.setUsers(usersBuilder.build());
                ps.close();
                rs.close();
                return groupBuilder.build();
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static List<String> findUserIdsByGroupId(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select user_id from t_join where course_id = ?");
            return getListById(id, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static void DbSynchronization(String id, String token, long dbVersion) {
        Connection conn = ConnectionUtil.getConn();
        try {
            ResultSet rs;
            PreparedStatement ps = conn.prepareStatement(
                    "select id from course inner join t_join on course_id = id where user_id = ?");
            ps.setString(1, id);
            List<String> list = getListById(id, ps);
            if (list.size() == 0) {
                return;
            }
            for (String courseId : list) {
                ps = conn.prepareStatement("select last_modified from course where id = ? and last_modified > ?");
                ps.setString(1, courseId);
                ps.setLong(2, dbVersion);
                rs = ps.executeQuery();
                if (rs.next()) {
                    PreparedStatement preparedStatement = conn.prepareStatement(
                            "insert into t_push (type, token, id1, time) values (?, ?, ?, ?)");
                    preparedStatement.setInt(1, 4);
                    preparedStatement.setString(2, token);
                    preparedStatement.setString(3, courseId);
                    preparedStatement.setLong(4, rs.getLong(1));
                    preparedStatement.executeUpdate();
                    preparedStatement.close();
                }
                ps.close();
                rs.close();
                ps = conn.prepareStatement("select id, time from " + courseId + "_m where time > ?");
                ps.setLong(1, dbVersion);
                rs = ps.executeQuery();
                while (rs.next()) {
                    PreparedStatement preparedStatement = conn.prepareStatement(
                            "insert into t_push (type, token, id1, id2, time) values (?, ?, ?, ?, ?)");
                    preparedStatement.setInt(1, 1);
                    preparedStatement.setString(2, token);
                    preparedStatement.setString(3, courseId);
                    preparedStatement.setLong(4, rs.getLong(1));
                    preparedStatement.setLong(5, rs.getLong(2));
                    preparedStatement.executeUpdate();
                    preparedStatement.close();
                }
                ps.close();
                rs.close();
                ps = conn.prepareStatement("select id, time from " + courseId + "_n where time > ?");
                ps.setLong(1, dbVersion);
                rs = ps.executeQuery();
                while (rs.next()) {
                    PreparedStatement preparedStatement = conn.prepareStatement(
                            "insert into t_push (type, token, id1, id2, time) values (?, ?, ?, ?, ?)");
                    preparedStatement.setInt(1, 2);
                    preparedStatement.setString(2, token);
                    preparedStatement.setString(3, courseId);
                    preparedStatement.setLong(4, rs.getLong(1));
                    preparedStatement.setLong(5, rs.getLong(2));
                    preparedStatement.executeUpdate();
                    preparedStatement.close();
                }
                ps.close();
                rs.close();
            }
            StringBuilder stringBuilder = new StringBuilder(
                    "select id, create_time from user inner join t_join on id = user_id " +
                            "where create_time > ? and course_id in (");
            int size = list.size();
            stringBuilder.append("?, ".repeat(Math.max(0, size - 1)));
            stringBuilder.append("?");
            stringBuilder.append(") group by id");
            ps = conn.prepareStatement(stringBuilder.toString());
            ps.setLong(1, dbVersion);
            for (int i = 1; i <= size; i++) {
                ps.setString(i + 1, list.get(i - 1));
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                PreparedStatement preparedStatement = conn.prepareStatement(
                        "insert into t_push (type, token, id1, time) values (?, ?, ?, ?)");
                preparedStatement.setInt(1, 3);
                preparedStatement.setString(2, token);
                preparedStatement.setString(3, rs.getString(1));
                preparedStatement.setLong(4, rs.getLong(2));
                preparedStatement.executeUpdate();
                preparedStatement.close();
            }
            ps.close();
            rs.close();
            ps = conn.prepareStatement("select last_modified from user where id = ? and last_modified > ?");
            ps.setString(1, id);
            ps.setLong(2, dbVersion);
            rs = ps.executeQuery();
            if (rs.next()) {
                PreparedStatement preparedStatement = conn.prepareStatement(
                        "insert into t_push (type, token, id1, time) values (?, ?, ?, ?)");
                preparedStatement.setInt(1, 5);
                preparedStatement.setString(2, token);
                preparedStatement.setString(3, id);
                preparedStatement.setLong(4, rs.getLong(1));
                preparedStatement.executeUpdate();
                preparedStatement.close();
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static DatagramProto.Notification insertNotification(DatagramProto.Notification notification) {
        int temporaryId = notification.getTemporaryId();
        String senderId = notification.getSenderId();
        String receiverId = notification.getReceiverId();
        String title = notification.getTitle();
        String content = notification.getContent();
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("insert into " + receiverId + "_n " +
                            "(sender_id, receiver_id, title, content, time, temporary_id) values (?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, senderId);
            ps.setString(2, receiverId);
            ps.setString(3, title);
            ps.setString(4, content);
            long time = System.currentTimeMillis();
            ps.setLong(5, time);
            ps.setInt(6, temporaryId);
            if (ps.executeUpdate() == 1) {
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    return DatagramProto.Notification.newBuilder().setId(rs.getLong(1)).setSenderId(senderId)
                            .setTime(time).setContent(content).setTitle(title).setReceiverId(receiverId)
                            .setTemporaryId(temporaryId).build();
                }
                rs.close();
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static DatagramProto.Message insertMessage(DatagramProto.Message message) {
        int temporaryId = message.getTemporaryId();
        String senderId = message.getSenderId();
        String receiverId = message.getReceiverId();
        String content = message.getContent();
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("insert into " + receiverId + "_m " +
                            "(sender_id, receiver_id, content, time, temporary_id) values (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, senderId);
            ps.setString(2, receiverId);
            ps.setString(3, content);
            long time = System.currentTimeMillis();
            ps.setLong(4, time);
            ps.setInt(5, temporaryId);
            if (ps.executeUpdate() == 1) {
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    return DatagramProto.Message.newBuilder().setId(rs.getInt(1)).setSenderId(senderId)
                            .setTime(time).setContent(content).setReceiverId(receiverId).setTemporaryId(temporaryId).build();
                }
                rs.close();
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static void insertPushItem(String token, int type, long time, String id1) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "insert into t_push (token, type, id1, time) values (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, token);
            ps.setInt(2, type);
            ps.setString(3, id1);
            ps.setLong(4, time);
            if (ps.executeUpdate() == 1) {
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    rs.getLong(1);
                    return;
                }
                rs.close();
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static void insertPushItem(String token, int type, long time, String id1, long id2) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "insert into t_push (token, type, id1, id2, time) values (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, token);
            ps.setInt(2, type);
            ps.setString(3, id1);
            ps.setLong(4, id2);
            ps.setLong(5, time);
            if (ps.executeUpdate() == 1) {
                ResultSet rs = ps.getGeneratedKeys();
                if (rs.next()) {
                    rs.getLong(1);
                    return;
                }
                rs.close();
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static void deletePushItem(long pushId) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select type, id1, id2 from t_push where id = ?");
            ps.setLong(1, pushId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                switch (rs.getInt(1)) {
                    case 1:
                        ps = conn.prepareStatement("update " + rs.getString(2) + "_m set temporary_id = null where id = ?");
                        ps.setLong(1, rs.getLong(3));
                        ps.executeUpdate();
                        break;
                    case 2:
                        ps = conn.prepareStatement("update " + rs.getString(2) + "_n set temporary_id = null where id = ?");
                        ps.setLong(1, rs.getLong(3));
                        ps.executeUpdate();
                        break;
                    default:
                        break;
                }
                ps = conn.prepareStatement("delete from t_push where id = ?");
                ps.setLong(1, pushId);
                ps.executeUpdate();
                ps.close();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static void deletePushItemByToken(String token) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("delete from t_push where token = ?");
            ps.setString(1, token);
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static List<String> findGroupIdsByUserId(String userId) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select course_id from t_join where user_id = ?");
            return getListById(userId, ps);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    private static List<String> getListById(String id, PreparedStatement ps) throws SQLException {
        ps.setString(1, id);
        ResultSet rs = ps.executeQuery();
        List<String> list = new ArrayList<>();
        while (rs.next()) {
            list.add(rs.getString(1));
        }
        ps.close();
        rs.close();
        return list;
    }

    public static DatagramProto.User findUserByIdSimply(String userId) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement("select create_time, name, identity from user where id = ?");
            ps.setString(1, userId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return DatagramProto.User.newBuilder().setId(userId).setName(rs.getString(2))
                        .setCreateTime(rs.getLong(1)).setIdentityValue(rs.getInt(3)).build();
            }
            ps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static PushItem findPushItemByToken(String token) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "select id, type, id1, id2 from t_push where token = ? order by time limit 1");
            ps.setString(1, token);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new PushItem(rs.getLong(1), rs.getInt(2), rs.getString(3),
                        rs.getLong(4));
            }
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
    }

    public static DatagramProto.Message findMessageById(String courseId, long messageId) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "select sender_id, receiver_id, content, time, temporary_id from " + courseId + "_m where id = ?");
            ps.setLong(1, messageId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return DatagramProto.Message.newBuilder().setId(messageId).setSenderId(rs.getString(1))
                        .setReceiverId(rs.getString(2)).setContent(rs.getString(3))
                        .setTime(rs.getLong(4)).setTemporaryId(rs.getInt(5)).build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static DatagramProto.Notification findNotificationById(String courseId, long notificationId) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "select sender_id, receiver_id, title, content, time, temporary_id from " + courseId + "_n where id = ?");
            ps.setLong(1, notificationId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return DatagramProto.Notification.newBuilder().setId(notificationId).setSenderId(rs.getString(1))
                        .setReceiverId(rs.getString(2)).setTitle(rs.getString(3))
                        .setContent(rs.getString(4)).setTime(rs.getLong(5))
                        .setTemporaryId(rs.getInt(6)).build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            ConnectionUtil.closeConn();
        }
        return null;
    }

    public static long findLastModifiedByUserId(String id) {
        Connection conn = ConnectionUtil.getConn();
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "select last_modified from user where id = ?");
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        } finally {
            ConnectionUtil.closeConn();
        }
        return 0;
    }

    public static boolean updatePhotoByUserId(String userId, ByteString bytes) {
        byte[] image = bytes.toByteArray();
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file;
        Connection conn = ConnectionUtil.getConn();
        try {
            File dir = new File("/usr/img");
            if(!dir.exists() && !dir.isDirectory()){
                dir.mkdirs();
            }
            file = new File("/usr/img" + File.separator + userId + "_img.jpg");
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(image);
            PreparedStatement ps = conn.prepareStatement("update user set last_modified = ? where id = ?");
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, userId);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConn();
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return false;
    }
}
