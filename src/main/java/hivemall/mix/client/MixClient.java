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

import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.MixedModel;
import hivemall.mix.NodeInfo;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

public final class MixClient extends AbstractMixClient {

    private final boolean ssl;
    private final MixRequestRouter router;
    private final MixClientHandler msgHandler;
    private final Map<NodeInfo, Channel> channelMap;

    private boolean initialized;
    private EventLoopGroup workers;

    public MixClient(
            @Nonnull MixEventName event,
            @CheckForNull String groupID,
            @Nonnull String connectURIs,
            boolean ssl,
            int mixThreshold,
            @Nonnull MixedModel model) {
        super(event, groupID, mixThreshold);
        this.ssl = ssl;
        this.router = new MixRequestRouter(connectURIs);
        this.msgHandler = new MixClientHandler(model);
        this.channelMap = new HashMap<NodeInfo, Channel>();
        this.initialized = false;
        this.workers = null;
    }

    @Override
    public MixClient open() throws Exception {
        final NodeInfo[] serverNodes = router.getAllNodes();
        workers = new NioEventLoopGroup(1);
        for(NodeInfo node : serverNodes) {
            channelMap.put(node, configureBootstrap(msgHandler, workers, node, ssl));
        }
        initialized = true;
        return this;
    }

    private static Channel configureBootstrap(
            MixClientHandler handler, EventLoopGroup workerGroup, NodeInfo server, boolean ssl)
            throws SSLException, InterruptedException {
        // Configure SSL
        final SslContext sslCtx;
        if(ssl) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.channel(NioSocketChannel.class);
        b.handler(new MixClientInitializer(handler, sslCtx));

        SocketAddress remoteAddr = server.getSocketAddress();
        ChannelFuture channelFuture = b.connect(remoteAddr).sync();
        return channelFuture.channel();
    }

    @Override
    protected void sendMsgToServer(MixMessage msg) throws InterruptedException {
        assert initialized;
        NodeInfo server = router.selectNode(msg);
        Channel ch = channelMap.get(server);
        // If not active, reconnect it
        if(!ch.isActive()) {
            SocketAddress remoteAddr = server.getSocketAddress();
            ch.connect(remoteAddr).sync();
        }
        // Send asynchronously in the background
        ch.writeAndFlush(msg);
    }


    @Override
    public void close() throws IOException {
        assert initialized;
        assert workers != null;
        for(Channel ch : channelMap.values()) {
            ch.close();
        }
        channelMap.clear();
        workers.shutdownGracefully();
        initialized = false;
    }

}
