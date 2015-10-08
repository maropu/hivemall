/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.client;

import hivemall.mix.AbstractMixMessageHandler;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.MixedModel;
import hivemall.mix.NodeInfo;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

public final class MixClientEx extends AbstractMixClient {

    private final String groupID;
    private final boolean ssl;
    private final MixRequestRouter router;
    private final MixClientHandler msgHandler;
    private final ConcurrentMap<NodeInfo, Channel> channelMap;

    private boolean initialized;
    private EventLoopGroup workers;

    public MixClientEx(
            @Nonnull MixEventName event,
            @CheckForNull String groupID,
            @Nonnull String connectURIs,
            boolean ssl,
            int mixThreshold,
            @Nonnull MixedModel model) {
        super(event, groupID, mixThreshold);
        this.groupID = groupID;
        this.ssl = ssl;
        this.router = new MixRequestRouter(connectURIs);
        this.msgHandler = new MixClientHandler(model);
        this.channelMap = new ConcurrentHashMap<NodeInfo, Channel>();
        this.initialized = false;
        this.workers = null;
    }

    @Override
    public MixClientEx open() throws Exception {
        final NodeInfo[] serverNodes = router.getAllNodes();
        workers = new NioEventLoopGroup(1);
        for(NodeInfo node : serverNodes) {
            connectMixWorker(node);
        }
        initialized = true;
        return this;
    }

    private boolean connectMixWorker(NodeInfo node)
            throws SSLException, InterruptedException {
        // Configure SSL
        final SslContext sslCtx;
        if(ssl) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }

        Bootstrap b = new Bootstrap();
        b.group(workers);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.channel(NioSocketChannel.class);
        b.handler(new MixClientInitializer(new MixWorkerLinkHandler(node), sslCtx));

        ChannelFuture channelFuture = b.connect(node.getSocketAddress()).sync();
        Channel ch = channelFuture.channel();
        // TODO: Need to compute necessary capacity
        // (cores/memoryMb) from MixedModel.
        MixMessage msg = new MixMessage(MixEventName.forkWorker, 1, 512);
        msg.setGroupID(groupID);
        ch.writeAndFlush(msg).sync();

        // Wait until workers get ready
        int retry = 0;
        while (channelMap.get(node) == null && retry++ < 120) { // Wait at most 1 min.
            Thread.sleep(500L);
        }
        return channelMap.get(node) != null;
    }

    @Sharable
    public final class MixWorkerLinkHandler extends AbstractMixMessageHandler {

        private final NodeInfo node;

        public MixWorkerLinkHandler(NodeInfo node) {
            this.node = node;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
            final MixEventName event = msg.getEvent();
            if (event != MixEventName.forkWorker) {
                throw new IllegalStateException("Unexpected event: " + event);
            }
            final int port = msg.getWorkerPort();
            Channel workerCh = initWorkerChannel(new NodeInfo(node.getAddress(), port));
            channelMap.put(node, workerCh);
        }

        private Channel initWorkerChannel(NodeInfo node) throws SSLException, InterruptedException {
            // Configure SSL
            final SslContext sslCtx;
            if(ssl) {
                sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
            } else {
                sslCtx = null;
            }

            Bootstrap b = new Bootstrap();
            b.group(workers);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.channel(NioSocketChannel.class);
            b.handler(new MixClientInitializer(msgHandler, sslCtx));

            ChannelFuture channelFuture = b.connect(node.getSocketAddress());
            return channelFuture.channel();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    @Override
    protected void sendMsgToServer(MixMessage msg) throws InterruptedException {
        assert initialized;
        NodeInfo worker = router.selectNode(msg);
        Channel ch = channelMap.get(worker);
        // If not active, reconnect it
        if(!ch.isActive()) {
            SocketAddress remoteAddr = worker.getSocketAddress();
            ch.connect(remoteAddr).sync();
        }
        // Send asynchronously in the background
        ch.writeAndFlush(msg);
    }

    @Override
    public void close() throws IOException {
        assert initialized;
        assert workers != null;
        channelMap.clear();
        workers.shutdownGracefully();
        initialized = false;
    }
}
