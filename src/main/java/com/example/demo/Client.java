package com.example.demo;

import com.example.demo.datagram.DatagramProto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {
    public static void main(String[] args) throws Exception{
        EventLoopGroup eventExecutors = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventExecutors).channel(NioSocketChannel.class).handler(new ClientInitializer());
            ChannelFuture channelFuture = bootstrap.connect("localhost", 8888).sync();
            Channel channel = channelFuture.channel();
            channel.writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setType(1).setSubtype(0).setId("1").build());
            channel.closeFuture().sync();
        } finally {
            eventExecutors.shutdownGracefully();
        }
        DatagramProto.Messages messages = DatagramProto.Messages.newBuilder().setMessages(0, DatagramProto.Message.newBuilder().setContent("")
        .setReceiverId("").setSenderId("").setTime(41).build()).setMessages(1, DatagramProto.Message.newBuilder().build()).build();
    }
}
