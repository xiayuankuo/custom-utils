package org.example.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
        if(Epoll.isAvailable()){
            //事件监听线程
//            bossGroup = new EpollEventLoopGroup(2, new DefaultThreadFactory());
            //工作线程
            workGroup = new EpollEventLoopGroup(NettyRuntime.availableProcessors());
        }else {
            bossGroup = new NioEventLoopGroup();
            workGroup = new NioEventLoopGroup();
        }
//        ServerBootstrap server = new ServerBootstrap()
//                .group(bossGroup, workGroup)
//                .childAttr()
//                .childOption()
//                .childHandler()
//                .group();
    }
}
