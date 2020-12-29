package com.example.demo.server;

import com.example.demo.utils.DaoUtil;
import com.example.demo.utils.UUId;
import com.example.demo.datagram.DatagramProto;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class ServerHandler extends SimpleChannelInboundHandler<DatagramProto.Datagram> {
    // 线程安全的连接池, 保存当前所有与服务器建立的连接, 连接建立时需要手动添加, 连接关闭时会自动移除
    private static ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /*
     * token池, 保存所有连接的token
     * 单例模式, 保证线程安全
     */
    private enum TokenPool {
        INSTANCE;
        private BiMap<String, String> token2Id;
        private BiMap<String, ChannelId> id2ChannelId;
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
         * @param token
         * @return 对应的用户id
         */
        public String findIdByToken(String token) {
            if (token2Id.containsKey(token)) {
                return token2Id.get(token);
            } else {
                return null;
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramProto.Datagram msg) throws Exception {
        // 获取报文版本
        final int version = msg.getVersion();
        switch (version) {
            case 1: // 处理版本1的报文
                version1Handler(ctx, DatagramProto.DatagramVersion1.parseFrom(msg.getDatagram()));
                break;
            default:
                break;
        }
    }

    /*
     * 处理版本1的报文
     */
    private void version1Handler(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final String token = msg.getToken();
        // 检查token是否合法
        if (msg.getType() != DatagramProto.DatagramVersion1.Type.LOGIN && !TokenPool.INSTANCE.checkToken(token, ctx.channel().id())) {
            return;
        }
        // 获取报文子类型
        final DatagramProto.DatagramVersion1.Subtype subtype = msg.getSubtype();
        switch (subtype) {
            case REQUEST: // 处理客户的Request请求
                version1Request(ctx, msg);
                break;
            case ACK: // 处理客户的Ack应答
                version1ACK(ctx, msg);
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
                        final String token = MessageDigest.getInstance("md5").digest(UUId.getInstance().getUniqID().getBytes()).toString();
                        TokenPool.INSTANCE.insert(token, username, ctx.channel().id());
                        // 发送Response报文, 返回token和正确码100
                        ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.LOGIN)
                                        .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(100).setToken(token).build().toByteString()
                        ).build());
                        // TODO : 登录后实现数据库同步
                        // long dbVersion = message.getDbVersion();
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
                            // 发送Response报文, 返回被添加的课程群信息和正确码101
                            /*
                            ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                                    DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.COURSE)
                                            .setToken(token).setOk(101).setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).
                                            .setXX(
                                                    TODO : 返回课程群信息
                                            )build().toByteString()
                            ).build());
                             */
                            // TODO : 添加课程群, 课程群推送
                            break;
                        default:
                            break;
                    }
                }
                break;
            }
            // 注册请求
            case REGISTER: {
                final String token = msg.getToken();
                final String id = TokenPool.INSTANCE.findIdByToken(token);
                if (DaoUtil.hasUserId(id)) { // 该学工号已注册
                    // 发送Response报文, 返回错误码200
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(203).build().toByteString()
                    ).build());
                } else if (DaoUtil.insertUser(msg.getRegister())){ // 注册成功
                    // 发送Response报文, 返回正确码100
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setToken(token).setOk(100).setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).build().toByteString()
                    ).build());
                    // TODO : 新用户推送
                } else { // 注册失败, 可能是身份与学工号不对应, 或学校数据库中查无此人, 或其他原因
                    // 发送Response报文, 返回错误码201
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.REGISTER)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.RESPONSE).setOk(204).build().toByteString()
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
                if (id != userId) { // 查询用户信息
                    DatagramProto.User user = DaoUtil.findUserById(userId, time);
                    if (user != null) { // 用户存在
                        if (user.getId() == null) { // 无需更新
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
                            if (msg.getUser().getType() == DatagramProto.User.UserType.STUDENT && DaoUtil.studentCheck(id)) { // 身份正确
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
                            } else if (msg.getUser().getType() == DatagramProto.User.UserType.TEACHER && DaoUtil.teacherCheck(id)) { // 身份正确
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
                            if (msg.getUser().getType() == DatagramProto.User.UserType.STUDENT && DaoUtil.studentCheck(id)) { // 身份正确
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
                            if (msg.getUser().getType() == DatagramProto.User.UserType.STUDENT && DaoUtil.studentCheck(id)) { // 身份正确
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
                // TODO : 添加消息, 消息推送
                break;
            }
            // 发布通知请求
            case NOTIFICATION: {
                // TODO : 添加通知, 通知推送
                break;
            }
            default:
                break;
        }
    }

    /*
     * 处理客户发来的版本1的Ack应答
     */
    private void version1ACK(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final DatagramProto.DatagramVersion1.Type type = msg.getType();
        switch (type) {
            case MESSAGE:
                switch (msg.getOk()) { // 用户收到消息推送
                    case 100:
                        // TODO
                        break;
                    default:
                        break;
                }
                break;
            case NOTIFICATION:
                switch (msg.getOk()) { // 用户收到通知推送
                    case 100:
                        // TODO
                        break;
                    default:
                        break;
                }
                break;
            case GROUP:
                switch (msg.getOk()) { // 用户收到群信息推送
                    case 100:
                        // TODO
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // 连接建立时, 向连接池中添加该连接
        channels.add(ctx.channel());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            switch (idleStateEvent.state()) {
                case WRITER_IDLE: // 一段时间内服务器没有向客户端发送数据, 则关闭连接
                    TokenPool.INSTANCE.remove(ctx.channel().id());
                    ctx.close();
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        // 出现异常, 连接关闭
        TokenPool.INSTANCE.remove(ctx.channel().id());
        ctx.close();
    }
}
