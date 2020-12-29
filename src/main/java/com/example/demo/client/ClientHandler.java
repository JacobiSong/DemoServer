package com.example.demo.client;

import com.example.demo.datagram.DatagramProto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.List;

public class ClientHandler extends SimpleChannelInboundHandler<DatagramProto.Datagram> {
    // 保存当前连接的token
    private static String token = "";

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
        // 检查token是否合法
        if (msg.getType() != DatagramProto.DatagramVersion1.Type.LOGIN && token != msg.getToken()) {
            return;
        }
        // 获取报文子类型
        final DatagramProto.DatagramVersion1.Subtype subtype = msg.getSubtype();
        switch (subtype) {
            case RESPONSE: // 处理服务器的Response响应报文
                version1Response(ctx, msg);
                break;
            case PUSH: // 处理服务器的Push推送报文
                version1Push(ctx, msg);
                break;
            default:
                break;
        }
    }

    /*
     * 处理服务器发来的版本1的Response响应报文
     */
    private void version1Response(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final DatagramProto.DatagramVersion1.Type type = msg.getType();
        switch (type) {
            case LOGIN: { // 登录响应
                switch (msg.getOk()) {
                    case 100: // 登录成功
                        token = msg.getToken();
                        // TODO : 通知UI线程登录成功
                        break;
                    case 201: // 密码错误
                        // TODO : 通知UI线程密码错误
                        break;
                    default:
                        break;
                }
                break;
            }
            case REGISTER: {
                switch (msg.getOk()) {
                    case 100:
                        // TODO : 通知UI线程注册成功
                        break;
                    case 203:
                        // TODO : 通知UI线程账号已被注册
                        break;
                    case 204:
                        // TODO : 通知UI线程注册失败（查无此人或身份错误，也有可能是其他未知原因）
                        break;
                    default:
                        break;
                }
                break;
            }
            case LOGOUT: {
                switch (msg.getOk()) {
                    case 100:
                        // TODO : 通知UI线程登出成功
                        break;
                    default:
                        break;
                }
                break;
            }
            case COURSE: {
                switch (msg.getOk()) {
                    case 100:
                        List<DatagramProto.Course> list = msg.getCourses().getCoursesList();
                        // TODO : 向UI线程传递可添加课程信息
                        break;
                    case 101:
                        // TODO : 添加课程群信息
                        // TODO : 通知UI线程课程群添加成功
                        break;
                    default:
                        break;
                }
                break;
            }
            case USER: {
                switch (msg.getOk()) {
                    case 100:
                        DatagramProto.User user = msg.getUser();
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 101:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 102:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 103:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 104:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 105:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 106:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 107:
                        // TODO : 更新本地数据库, 通知UI线程更新
                        break;
                    case 200:
                        // TODO : 无此用户
                        break;
                    case 201:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 202:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 203:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 204:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 205:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 206:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 207:
                        // TODO : 通知UI线程更新失败
                        break;
                    case 210:
                        // TODO : 身份错误
                        break;
                    case 300:
                        // TODO : 无需更新
                        break;
                    default:
                        break;
                }
                break;
            }
            case MESSAGE: {
                // TODO
                break;
            }
            case NOTIFICATION: {
                // TODO
                break;
            }
            default:
                break;
        }
    }

    /*
     * 处理服务器发来的版本1的Push推送报文
     */
    private void version1Push(ChannelHandlerContext ctx, DatagramProto.DatagramVersion1 msg) {
        final DatagramProto.DatagramVersion1.Type type = msg.getType();
        switch (type) {
            case MESSAGE:
                DatagramProto.Message message = msg.getMessage();
                // TODO : 更新本地缓存, 向UI线程推送消息
                // 发送Ack报文, 返回正确码100
                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.MESSAGE)
                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.ACK).setToken(token).setOk(100).build().toByteString()
                ).build());
                break;
            case NOTIFICATION:
                DatagramProto.Notification notification = msg.getNotification();
                // TODO : 更新本地缓存, 向UI线程推送通知
                // 发送Ack报文, 返回正确码100
                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                        DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.NOTIFICATION)
                                .setSubtype(DatagramProto.DatagramVersion1.Subtype.ACK).setToken(token).setOk(100).build().toByteString()
                ).build());
                break;
            default:
                break;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        // 捕获异常, 关闭连接
        token = "";
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent && token != null) {
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            switch (idleStateEvent.state()) {
                case ALL_IDLE: // 一段时间内连接不活跃, 发送保活报文
                    ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                            DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.KEEP_ALIVE)
                                    .setSubtype(DatagramProto.DatagramVersion1.Subtype.REQUEST).setToken(token).build().toByteString()
                    ).build());
                    break;
                default:
                    break;
            }
        }
    }
}