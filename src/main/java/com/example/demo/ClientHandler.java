package com.example.demo;

import com.example.demo.datagram.DatagramProto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

public class ClientHandler extends SimpleChannelInboundHandler<DatagramProto.Datagram> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramProto.Datagram msg) throws Exception {
        final int version = msg.getVersion();
        final int type = msg.getType();
        final int subtype = msg.getSubtype();
        if (version == 1) {
            if(type == 0) {

            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
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

    private void handleReaderIdle(ChannelHandlerContext ctx) {

    }

    private void handleWriteIdle(ChannelHandlerContext ctx) {

    }

    private void handleAllIdle(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setType(0).setSubtype(0));
    }
}
