package com.example.demo.server;

import com.example.demo.utils.DaoUtil;
import com.example.demo.utils.UUId;
import com.example.demo.datagram.DatagramProto;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class ServerHandler extends SimpleChannelInboundHandler<DatagramProto.Datagram> {
    // 线程安全的连接池, 保存当前所有与服务器建立的连接, 连接建立时需要手动添加, 连接关闭时会自动移除
    private static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /*
     * token池, 保存所有连接的token
     * 单例模式, 保证线程安全
     */
    private enum TokenPool {
        INSTANCE;
        private final BiMap<String, String> token2Id;
        private final BiMap<String, ChannelId> id2ChannelId;
        TokenPool() {
            token2Id = HashBiMap.create();
            id2ChannelId = HashBiMap.create();
        }

        /**
         * 添加一个token
         * @param token 要添加的token
         * @param id 对应的用户id
         * @param channelId 对应的连接id
         */
        public void insert(String token, String id, ChannelId channelId) {
            token2Id.forcePut(token, id);
            id2ChannelId.forcePut(id, channelId);
        }

        /**
         * 删除一个token
         * @param token 要删除的token
         */
        public void remove(String token) {
            if (token2Id.containsKey(token)) {
                id2ChannelId.remove(token2Id.get(token));
                token2Id.remove(token);
            }
        }

        /**
         * 删除一个连接对应的token
         * @param channelId 要删除的连接id
         */
        public void remove(ChannelId channelId) {
            if (id2ChannelId.containsValue(channelId)) {
                token2Id.inverse().remove(id2ChannelId.inverse().get(channelId));
                id2ChannelId.inverse().remove(channelId);
            }
        }

        /**
         * 检查token是否合法
         * @param token 待检查的token
         * @param channelId 待检查的连接id
         * @return true 合法, false 非法
         */
        public boolean checkToken(String token, ChannelId channelId) {
            if (token != null && token2Id.containsKey(token)) {
                return id2ChannelId.get(token2Id.get(token)).equals(channelId);
            }
            return false;
        }

        /**
         * 根据token查询对应的用户id
         * @param token token
         * @return (1) 对应的用户id (2) null token池中不存在该token
         */
        public String findIdByToken(String token) {
            return token2Id.getOrDefault(token, null);
        }

        /**
         * 根据用户id查询对应的token
         * @param id 用户id
         * @return (1) 对应的token (2) null 如果该用户不处于连接状态
         */
        public String findTokenById(String id) {
            return token2Id.inverse().getOrDefault(id, null);
        }

        /**
         * 根据用户id查询对应的连接id
         * @param id 用户id
         * @return (1) 对应的连接id (2) null 如果该用户不处于连接状态
         */
        public ChannelId findChannelIdById(String id) {
            return id2ChannelId.getOrDefault(id, null);
        }

        public String findTokenByChannelId(ChannelId channelId) {
            String id = id2ChannelId.inverse().getOrDefault(channelId, null);
            return token2Id.inverse().getOrDefault(id, null);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramProto.Datagram msg) throws Exception {
        // 获取报文版本
        final int version = msg.getVersion();
        if (version == 1) { // 处理版本1的报文
            version1Handler(ctx, DatagramProto.DatagramVersion1.parseFrom(msg.getDatagram()));
        }
    }

    /*
     * 处理版本1的报文
     */
    private void version1Handler(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final String token = msg.getToken();
        // 检查token是否合法
        if (msg.getType() != DatagramProto.DatagramVersion1.Type.LOGIN && msg.getType() != DatagramProto.DatagramVersion1.Type.REGISTER
                && !TokenPool.INSTANCE.checkToken(token, ctx.channel().id())) {
            return;
        }
        // 获取报文子类型
        final DatagramProto.DatagramVersion1.Subtype subtype = msg.getSubtype();
        switch (subtype) {
            case REQUEST: // 处理客户的Request请求
                version1Request(ctx, msg);
                break;
            case ACK: // 处理客户的Ack应答
                version1ACK(msg);
                break;
            default:
                break;
        }
    }

    /*
     * 处理客户发来的版本1的Request请求
     */
    private void version1Request(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final DatagramProto.DatagramVersion1.Type type = msg.getType();
        // Request报文处理
        switch (type) {
            // 登录请求
            case LOGIN: {
                DatagramProto.Login message = msg.getLogin();
                String username = message.getUsername();
                String password = message.getPassword();
                if (DaoUtil.loginCheck(username, password)) { // 密码正确
                    try { // 生成token
                        final String token = Arrays.toString(MessageDigest.getInstance("md5").digest(UUId.getInstance().getUniqID().getBytes()));
                        TokenPool.INSTANCE.insert(token, username, ctx.channel().id());
                        // 发送Response报文, 返回token和正确码100
                        ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.LOGIN)
                                        .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(100).setToken(token).build().toByteString()
                        ).build());
                        DaoUtil.DbSynchronization(username, token, message.getDbVersion());
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                        break;
                    }
                } else { // 密码错误
                    // 发送Response报文, 返回错误码200
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.LOGIN)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(201).build().toByteString()
                    ).build());
                }
                break;
            }
            // 登出请求
            case LOGOUT: {
                final String token = msg.getToken();
                // 发送Response报文, 返回正确码100
                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.LOGOUT)
                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(100).setToken(token).build().toByteString()
                ).build());
                DaoUtil.deletePushItemByToken(token);
                TokenPool.INSTANCE.remove(token);
                ctx.close();
                break;
            }
            // 课程群相关请求
            case COURSE: {
                final String token = msg.getToken();
                final String id = TokenPool.INSTANCE.findIdByToken(token);
                if (DaoUtil.teacherCheck(id)) { // 身份是老师
                    switch (msg.getOk()) { // 获取业务代码
                        case 100: // 获取可添加课程群请求
                            // 发送Response报文, 返回可添加的课程信息和正确码100
                            List<DatagramProto.Course> list = DaoUtil.findCoursesByTeacherId(id);
                            ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                    DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.COURSE)
                                            .setToken(token).setOk(100).setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE)
                                            .setCourses(
                                                    DatagramProto.Courses.newBuilder().addAllCourses(list).build()
                                            ).build().toByteString()
                            ).build());
                            break;
                        case 101: // 添加课程群请求
                            String courseId = msg.getCourse().getId();
                            if (DaoUtil.hasGroupId(id)) { // 该群已经被创建了
                                // 发送Response报文, 返回错误码200
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.COURSE)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(200).build().toByteString()
                                ).build());
                            } else if (DaoUtil.insertGroup(id, courseId)){ // 添加成功
                                // 发送Response报文, 返回被添加的课程群信息和正确码101
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.COURSE)
                                                .setToken(token).setOk(101).setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE)
                                                .setGroup(DaoUtil.findGroupById(courseId)).build().toByteString()
                                ).build());
                                // TODO : 课程群推送
                            } else { // 未知原因添加失败
                                // 发送Response报文, 返回错误码201
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.COURSE)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(201).build().toByteString()
                                ).build());
                            }
                            break;
                        default:
                            break;
                    }
                }
                break;
            }
            // 注册请求
            case REGISTER: {
                final String id = msg.getRegister().getUsername();
                if (DaoUtil.hasUserId(id)) { // 该学工号已注册
                    // 发送Response报文, 返回错误码200
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(200).build().toByteString()
                    ).build());
                } else if (DaoUtil.insertUser(msg.getRegister())){ // 注册成功
                    // 发送Response报文, 返回正确码100
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setOk(100).setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).build().toByteString()
                    ).build());
                    // TODO : 新用户推送
                    // TODO : 新用户被推送
                } else { // 注册失败, 可能是身份与学工号不对应, 或学校数据库中查无此人, 或其他原因
                    // 发送Response报文, 返回错误码201
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(201).build().toByteString()
                    ).build());
                }
                break;
            }
            // 保活请求
            case KEEP_ALIVE: {
                // 发送Response报文, 返回正确码100
                final String token = msg.getToken();
                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.KEEP_ALIVE)
                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(100).setToken(token).build().toByteString()
                ).build());
                break;
            }
            // 用户资料相关请求
            case USER: {
                final String token = msg.getToken();
                final String id = TokenPool.INSTANCE.findIdByToken(token);
                final String userId = msg.getUser().getId();
                final long time = msg.getUser().getLastModified();
                if (!userId.isEmpty() && !userId.equals(id)) { // 查询用户信息
                    DatagramProto.User user = DaoUtil.findUserById(userId, time);
                    if (user != null) { // 用户存在
                        if (user.getId().isEmpty()) { // 无需更新
                            // 发送Response报文, 返回代码300
                            ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                    DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                            .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(300).setToken(token)
                                            .build().toByteString()
                            ).build());
                        } else {
                            // 发送Response报文, 返回用户信息和正确码100
                            ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                    DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                            .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(100).setToken(token)
                                            .setUser(user).build().toByteString()
                            ).build());
                        }
                    } else { // 无此用户
                        // 发送Response报文, 返回错误码200
                        ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                        .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(202).setToken(token).build().toByteString()
                        ).build());
                    }
                } else {
                    switch (msg.getOk()) {
                        case 101: // 修改手机号
                            if (DaoUtil.updatePhoneById(id, msg.getUser().getPhone())) { // 修改成功
                                // 发送Response报文, 返回正确代码101
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(101).setToken(token)
                                                .build().toByteString()
                                ).build());
                            } else { // 未知原因修改失败
                                System.out.println(201);
                                // 发送Response报文, 返回错误代码201
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(201).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 102: // 修改电子邮箱地址
                            if (DaoUtil.updateEmailById(id, msg.getUser().getEmail())) { // 修改成功
                                // 发送Response报文, 返回正确代码102
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(102).setToken(token)
                                                .build().toByteString()
                                ).build());
                            } else { // 未知原因修改失败
                                // 发送Response报文, 返回错误代码202
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(202).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 103: // 修改性别
                            if (DaoUtil.updateGenderById(id, msg.getUser().getGender())) { // 修改成功
                                // 发送Response报文, 返回正确代码103
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(103).setToken(token)
                                                .build().toByteString()
                                ).build());
                            } else { // 未知原因修改失败
                                // 发送Response报文, 返回错误代码203
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(203).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 104: // 修改密码
                            if (DaoUtil.updatePasswordById(id, msg.getUser().getPassword())) { // 修改成功
                                // 发送Response报文, 返回正确代码104
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(104).setToken(token)
                                                .build().toByteString()
                                ).build());
                            } else { // 未知原因修改失败
                                // 发送Response报文, 返回错误代码204
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(204).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 105: // 修改院系
                            if (DaoUtil.studentCheck(id)) { // 身份为学生
                                if (DaoUtil.updateDepartmentByStudentId(id, msg.getUser().getStudent().getDepartment())) { // 修改成功
                                    // 发送Response报文, 返回正确代码105
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(105).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                } else { // 未知原因修改失败
                                    // 发送Response报文, 返回错误代码205
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(205).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                }
                            } else if (DaoUtil.teacherCheck(id)) { // 身份为老师
                                if (DaoUtil.updateDepartmentByTeacherId(id, msg.getUser().getTeacher().getDepartment())) { // 修改成功
                                    // 发送Response报文, 返回正确代码105
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(105).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                } else { // 未知原因修改失败
                                    // 发送Response报文, 返回错误代码205
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(205).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                }
                            } else { // 身份错误
                                // 发送Response报文, 返回错误代码210
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(210).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 106: // 修改专业
                            if (DaoUtil.studentCheck(id)) { // 身份正确
                                if (DaoUtil.updateMajorById(id, msg.getUser().getStudent().getMajor())) { // 修改成功
                                    // 发送Response报文, 返回正确代码106
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(106).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                } else { // 未知原因修改失败
                                    // 发送Response报文, 返回错误代码206
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(206).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                }
                            } else { // 身份错误
                                // 发送Response报文, 返回错误代码210
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(210).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        case 107: // 修改班级
                            if (DaoUtil.studentCheck(id)) { // 身份正确
                                if (DaoUtil.updateClassNoById(id, msg.getUser().getStudent().getClassNo())) { // 修改成功
                                    // 发送Response报文, 返回正确代码107
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(107).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                } else { // 未知原因修改失败
                                    // 发送Response报文, 返回错误代码207
                                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(207).setToken(token)
                                                    .build().toByteString()
                                    ).build());
                                }
                            } else { // 身份错误
                                // 发送Response报文, 返回错误代码210
                                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.USER)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(210).setToken(token)
                                                .build().toByteString()
                                ).build());
                            }
                            break;
                        default:
                            break;
                    }
                }
                break;
            }
            // 发送消息请求
            case MESSAGE: {
                String token = msg.getToken();
                DatagramProto.Message message = DaoUtil.insertMessage(msg.getMessage());
                if (message != null) { // 发送成功
                    // 在数据库中添加推送条目
                    long push = DaoUtil.insertPushItem(token, 1, message.getReceiverId(), message.getId());
                    // 发送Response报文, 返回正确码100和更新后的消息
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setToken(token).setOk(100).setPush(push)
                                    .setType(DatagramProto.DatagramVersion1.Type.MESSAGE).setMessage(message)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).build().toByteString()
                    ).build());
                    // 消息推送
                    List<String> list = DaoUtil.findUserIdsByGroupId(message.getReceiverId());
                    list.remove(message.getSenderId()); // 移除发送者
                    for (String userId : list) { // 循环其余所有的用户
                        token = TokenPool.INSTANCE.findTokenById(userId);
                        ChannelId channelId = TokenPool.INSTANCE.findChannelIdById(userId);
                        if (token != null && channelId != null) { // 如果群中某成员在线, 则推送消息
                            Channel channel = channels.find(channelId);
                            if (channel != null) {
                                // 在数据库中添加推送条目
                                push = DaoUtil.insertPushItem(token, 1, message.getReceiverId(), message.getId());
                                channel.writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.MESSAGE)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.PUSH).setToken(token).setOk(100)
                                                .setMessage(message).setPush(push).build().toByteString()
                                ));
                            }
                        }
                    }
                } else { // 因未知原因发送失败
                    // 发送Response报文, 返回错误码200
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setToken(msg.getToken())
                                    .setType(DatagramProto.DatagramVersion1.Type.MESSAGE).setOk(200)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).build().toByteString()
                    ).build());
                }
                break;
            }
            // 发布通知请求
            case NOTIFICATION: {
                String token = msg.getToken();
                DatagramProto.Notification notification = DaoUtil.insertNotification(msg.getNotification());
                if (notification != null) { // 发送成功
                    long push = DaoUtil.insertPushItem(token, 2, notification.getReceiverId(), notification.getId());
                    // 发送Response报文, 返回正确码100和通知新信息
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setToken(msg.getToken())
                                    .setType(DatagramProto.DatagramVersion1.Type.NOTIFICATION).setOk(100).setPush(push)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setNotification(notification)
                                    .build().toByteString()
                    ).build());
                    // 通知推送
                    List<String> list = DaoUtil.findUserIdsByGroupId(notification.getReceiverId());
                    list.remove(notification.getSenderId()); // 移除发送者
                    for (String userId : list) { // 循环其余所有的用户
                        token = TokenPool.INSTANCE.findTokenById(userId);
                        ChannelId channelId = TokenPool.INSTANCE.findChannelIdById(userId);
                        if (token != null && channelId != null) { // 如果群中某成员在线, 则推送消息
                            Channel channel = channels.find(channelId);
                            if (channel != null) {
                                // 在数据库中添加推送条目
                                push = DaoUtil.insertPushItem(token, 2, notification.getReceiverId(), notification.getId());
                                channel.writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.MESSAGE)
                                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.PUSH).setToken(token).setOk(100)
                                                .setNotification(notification).setPush(push).build().toByteString()
                                ));
                            }
                        }
                    }
                }
                else { // 因未知原因发送失败
                    // 发送Response报文, 返回错误码200
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setToken(msg.getToken())
                                    .setType(DatagramProto.DatagramVersion1.Type.NOTIFICATION).setOk(200)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).build().toByteString()
                    ).build());
                }
                break;
            }
            default:
                break;
        }
    }

    /*
     * 处理客户发来的版本1的Ack应答
     */
    private void version1ACK(DatagramProto.DatagramVersion1 msg) {
        DaoUtil.deletePushItem(msg.getPush());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // 连接建立时, 向连接池中添加该连接
        channels.add(ctx.channel());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            if (idleStateEvent.state() == IdleState.WRITER_IDLE) { // 一段时间内服务器没有向客户端发送数据, 则关闭连接
                String token = TokenPool.INSTANCE.findTokenByChannelId(ctx.channel().id());
                DaoUtil.deletePushItemByToken(token);
                TokenPool.INSTANCE.remove(ctx.channel().id());
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        // 出现异常, 连接关闭
        String token = TokenPool.INSTANCE.findTokenByChannelId(ctx.channel().id());
        DaoUtil.deletePushItemByToken(token);
        TokenPool.INSTANCE.remove(ctx.channel().id());
        ctx.close();
    }
}
