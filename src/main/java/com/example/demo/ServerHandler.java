package com.example.demo;

import com.example.demo.datagram.DatagramProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ServerHandler extends SimpleChannelInboundHandler<DatagramProto.Datagram> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramProto.Datagram msg) {
        final int version = msg.getVersion();
        final int type = msg.getType();
        final int subtype = msg.getSubtype();
        if (version == 1) {
            if (type == 0 && subtype == 0) {
                ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setType(0).setSubtype(1));
            }
            else if (type == 1 && subtype == 0) { // 获取课程信息
                new Thread(new Runnable() {
                    @Override
                    public void run () {
                        String id = msg.getId();
                        if (id == null) {
                            id = "";
                        }
                        List<DatagramProto.Course> list = new ArrayList<>();
                        try {
                            list = new Dao().getCoursesById(id);
                            for (DatagramProto.Course course : list) {
                                System.out.println(course.getId() + " " + course.getClassroom());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            switch (idleStateEvent.state()) {
                case READER_IDLE:
                    handleReaderIdle(ctx);
                    break;
                case WRITER_IDLE:
                    handleWriteIdle(ctx);
                    break;
                case ALL_IDLE:
                    handleAllIdle(ctx);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    private void handleReaderIdle(ChannelHandlerContext ctx) {
        ctx.close();
    }

    private void handleWriteIdle(ChannelHandlerContext ctx) {

    }

    private void handleAllIdle(ChannelHandlerContext ctx) {

    }
}
