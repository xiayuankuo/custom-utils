package org.example.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * @author yuankuo.xia
 * @Date 2024/6/11
 */
public class NettyApp {
    public static void startServer(){
        EventLoopGroup bossGroup;
        EventLoopGroup workGroup;
        Class<? extends ServerChannel> channelClass;
        if(Epoll.isAvailable()){
            //事件监听线程
            bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("event default"));
            //工作线程
            workGroup = new EpollEventLoopGroup(NettyRuntime.availableProcessors(), new DefaultThreadFactory("worker default"));
            channelClass = EpollServerSocketChannel.class;
        }else {
            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("event default"));
            workGroup = new NioEventLoopGroup(NettyRuntime.availableProcessors(), new DefaultThreadFactory("worker default"));
            channelClass = NioServerSocketChannel.class;
        }
        ServerBootstrap server = new ServerBootstrap()
                .group(bossGroup, workGroup)
//                .channel(channelClass)
//                .childAttr()
//                .childOption()
//                .childHandler()
//                .handlerAdded()
//                .group()
                ;

        server.bind(8023);
    }
}
