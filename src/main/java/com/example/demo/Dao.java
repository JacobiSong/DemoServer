package com.example.demo;

import com.example.demo.datagram.DatagramProto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Dao {
    public List<DatagramProto.Course> getCoursesById(String userId) throws SQLException {
        List<DatagramProto.Course> list = new ArrayList<DatagramProto.Course>();
        Connection conn = ConnectionUtil.getConn();
        String sql="select * from course inner join join_courses_with_teachers j on course.id = j.course_id where j.user_id = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, userId);
        ResultSet rs=ps.executeQuery();
        while (rs.next()) {
            String remarks = rs.getString("remarks");
            if (remarks == null) {
                remarks = "";
            }
            DatagramProto.Course course = DatagramProto.Course.newBuilder().setId(rs.getString("id"))
                    .setName(rs.getString("name")).setClassroom(rs.getString("classroom"))
                    .setTime(rs.getString("time")).setSemester(rs.getInt("semester"))
                    .setHasGroup(rs.getBoolean("has_group")).setRemarks(remarks).build();
            list.add(course);
        }
        ConnectionUtil.closeConn();
        return list;
    }
}
