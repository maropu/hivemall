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
package hivemall.mix.network;

import hivemall.utils.collections.TimestampedValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

public final class MixServerRequestHandler {

    public final static class MixServerRequestReceiver
            extends SimpleChannelInboundHandler<MixServerRequest> {

        final ConcurrentMap<ContainerId, TimestampedValue<NodeId>> activeMixServers;

        public MixServerRequestReceiver(
                ConcurrentMap<ContainerId, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixServerRequest req)
                throws Exception {
            // TODO: Return only # of requested resources
            int numServers = 0;
            StringBuilder urls = new StringBuilder();
            for (TimestampedValue<NodeId> value: activeMixServers.values()) {
                final NodeId node = value.getValue();
                urls.append(node);
                urls.append(",");
            }
            ctx.write(new MixServerRequest(numServers, urls.toString()));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    public final static class MixServerRequestInitializer
            extends ChannelInitializer<SocketChannel> {

        private final MixServerRequestReceiver handler;

        public MixServerRequestInitializer(MixServerRequestReceiver handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new RequestDecoder(), handler);
        }
    }

    private final static class RequestDecoder extends LengthFieldBasedFrameDecoder {

        public RequestDecoder() {
             super(65536, 0, 4, 0, 4);
        }

        @Override
        protected MixServerRequest decode(ChannelHandlerContext ctx, ByteBuf in)
                throws Exception {
            final ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            int numRequest = frame.readInt();
            String URIs = readString(frame);
            return new MixServerRequest(numRequest, URIs);
        }

        private String readString(final ByteBuf in) {
            int length = in.readInt();
            if(length == -1) {
                return null;
            }
            byte[] b = new byte[length];
            in.readBytes(b, 0, length);
            try {
                return new String(b, "utf-8");
            } catch (UnsupportedEncodingException e) {
                return null;
            }
         }
    }
}
