package com.example.demo.client;

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
            System.out.println("---登录---");
            channel.writeAndFlush(DatagramProto.Datagram.newBuilder().setVersion(1).setDatagram(
                    DatagramProto.DatagramVersion1.newBuilder().setType(DatagramProto.DatagramVersion1.Type.LOGIN)
                        .setSubtype(DatagramProto.DatagramVersion1.Subtype.REQUEST).setLogin(
                                DatagramProto.Login.newBuilder().setUsername("testuser").setPassword("testuser")
                            .setDbVersion(0).build()
                    ).build().toByteString()
            ).build());
            channel.closeFuture().sync();
        } finally {
            eventExecutors.shutdownGracefully();
        }
        DatagramProto.Messages messages = DatagramProto.Messages.newBuilder().setMessages(0, DatagramProto.Message.newBuilder().setContent("")
        .setReceiverId("").setSenderId("").setTime(41).build()).setMessages(1, DatagramProto.Message.newBuilder().build()).build();
    }
}
