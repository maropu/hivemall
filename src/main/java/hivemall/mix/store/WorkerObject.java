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
package hivemall.mix.store;

import com.sun.istack.Nullable;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.AbstractMixMessageHandler;
import hivemall.mix.server.MixServer.ServerState;

import hivemall.mix.server.MixServerInitializer;
import hivemall.utils.net.NetUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public final class WorkerObject implements Closeable {

    private final int port;
    private final EventLoopGroup workerGroup;
    private final int cores;
    private final int memoryMb;

    private AtomicInteger coresUsed;
    private AtomicInteger memoryMbUsed;
    private volatile ServerState state;

    @Nullable
    private Channel channel;

    public WorkerObject(
            @Nonnull EventLoopGroup workerGroup,
            int cores,
            int memoryMb,
            AtomicInteger coresUsed,
            AtomicInteger memoryMbUsed) {
        this.port = NetUtils.getAvailablePort();
        this.workerGroup = workerGroup;
        this.cores = cores;
        this.memoryMb = memoryMb;
        this.coresUsed = coresUsed;
        this.memoryMbUsed = memoryMbUsed;
        this.state = ServerState.INITIALIZING;
        this.channel = null;
    }

    public ServerState getState() {
        return state;
    }

    public int getPort() {
        return port;
    }

    public synchronized void waitForRunning()
            throws IllegalStateException, InterruptedException {
        switch (state) {
            case INITIALIZING: {
                Bootstrap b = new Bootstrap();
                b.group(workerGroup);
                b.option(ChannelOption.SO_KEEPALIVE, true);
                b.option(ChannelOption.TCP_NODELAY, true);
                b.channel(NioSocketChannel.class);
                b.handler(new MixServerInitializer(new WorkerWatcher(this)));
                final InetSocketAddress addr = NetUtils.getInetSocketAddress("localhost", port);

                ChannelFuture channelFuture = null;
                int retry = 0;
                while (retry++ < 120) { // Wait at most 1 min.
                    Thread.sleep(500L);
                    try {
                        channelFuture = b.connect(addr).sync();
                        break;
                    } catch (InterruptedException e1) {
                        throw e1;
                    } catch (Exception e2) {
                        // Worker probably haven't got awaken, so retry it
                    }
                }

                // If not connected to a worker, throw an exception
                if (channelFuture == null) {
                    throw new IllegalStateException(
                            "Can't connect a forked worker (timeout)");
                }

                channel = channelFuture.channel();
                channel.writeAndFlush(new MixMessage(MixEventName.ping)).sync();
                retry = 0;
                while(state != ServerState.RUNNING && retry++ < 32) {
                    Thread.sleep(100L);
                }
                if (state != ServerState.RUNNING) {
                    throw new IllegalStateException(
                            "Worker state is not running (timeout)");
                }
                break;
            }
            case RUNNING: {
                break;
            }
            case STOPPING: {
                throw new IllegalStateException("Worker already has been stopped");
            }
            default: {
                throw new IllegalStateException("Unkown state: " + state);
            }
        }
    }

    public void setState(ServerState state) {
        this.state = state;
    }

    public int getCores() {
        return cores;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    @ChannelHandler.Sharable
    private final class WorkerWatcher extends AbstractMixMessageHandler {

        private final WorkerObject worker;

        public WorkerWatcher(WorkerObject self) {
            this.worker = self;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
            final MixEventName event = msg.getEvent();
            if (event != MixMessage.MixEventName.ack) {
                throw new IllegalStateException("Unexpected event: " + event);
            }
            worker.setState(ServerState.RUNNING);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    @Override
    public void close() {
        state = ServerState.STOPPING;
        if (channel != null) {
            try {
                channel.writeAndFlush(new MixMessage(MixEventName.killWorker)).sync();
            } catch (InterruptedException e) {
                // Ignore any exception
            }
            channel.close();
        }
        // Release the given resources
        coresUsed.addAndGet(-cores);
        memoryMbUsed.addAndGet(-memoryMb);
    }
}
