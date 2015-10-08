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
package hivemall.mix.server;

import hivemall.mix.metrics.MetricsRegistry;
import hivemall.mix.metrics.MixServerMetrics;
import hivemall.mix.metrics.ThroughputCounter;
import hivemall.mix.store.SessionStore;
import hivemall.mix.store.SessionStore.IdleSessionSweeper;
import hivemall.mix.store.WorkerHandler;
import hivemall.mix.store.WorkerHandler.StoppedWorkerSweeper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.io.Closeable;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

public final class MixServer implements Runnable, Closeable {

    public static final int DEFAULT_PORT = 11212;

    private final MixServerArguments options;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private volatile ServerState state;

    public MixServer(String[] args) {
        this.options = new MixServerArguments(args);
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(options.getCores());
        this.state = ServerState.INITIALIZING;
    }

    public enum ServerState {
        INITIALIZING, RUNNING, STOPPING,
    }

    public static void main(String[] args) {
        new MixServer(args).run();
    }

    public ServerState getState() {
        return state;
    }

    @Override
    public void run() {
        try {
            start();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (SSLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void start() throws CertificateException, SSLException, InterruptedException {
        if (options.isFork()) {
            final ScheduledExecutorService idleSessionChecker =
                    Executors.newScheduledThreadPool(1);
            final ScheduledExecutorService stoppedWorkerChecker =
                    Executors.newScheduledThreadPool(1);
            final WorkerHandler workerHandler =
                    new WorkerHandler(options, options.getCores(), options.getMemoryMb());
            try {
                // Start idle session sweeper
                SessionStore sessions = new SessionStore();
                Runnable cleanSessionTask = new IdleSessionSweeper(
                        sessions, options.getSessionTTLinSec() * 1000L);
                idleSessionChecker.scheduleAtFixedRate(
                        cleanSessionTask,
                        options.getSessionTTLinSec() + 10L,
                        options.getSweepSessionIntervalInSec(),
                        TimeUnit.SECONDS);

                // Start a sweeper for stopped workers
                Runnable cleanWorkerTask = new StoppedWorkerSweeper(workerHandler);
                stoppedWorkerChecker.scheduleAtFixedRate(
                        cleanWorkerTask,
                        options.getSweepWorkerIntervalInSec() + 10L,
                        options.getSweepWorkerIntervalInSec(),
                        TimeUnit.SECONDS);

                // Accept connections
                acceptConnections(new ForkWorkerHandler(
                        workerHandler, sessions, options.getSyncThreshold(),
                        options.getScale()));
            } finally {
                // Release threads
                idleSessionChecker.shutdownNow();
                stoppedWorkerChecker.shutdown();
                workerHandler.close();
            }
        } else {
            final ScheduledExecutorService idleSessionChecker =
                    Executors.newScheduledThreadPool(1);
            try {
                // Start idle session sweeper
                SessionStore sessions = new SessionStore();
                Runnable cleanSessionTask = new IdleSessionSweeper(
                        sessions, options.getSessionTTLinSec() * 1000L);
                idleSessionChecker.scheduleAtFixedRate(
                        cleanSessionTask,
                        options.getSessionTTLinSec() + 10L,
                        options.getSweepSessionIntervalInSec(),
                        TimeUnit.SECONDS);

                // Accept connections
                acceptConnections(new MixWorkerHandler(
                        sessions, options.getSyncThreshold(),
                        options.getScale()));
            } finally {
                // Release threads
                idleSessionChecker.shutdownNow();
            }
        }
    }

    private void acceptConnections(@Nonnull BaseMixHandler msgHandler)
            throws CertificateException, SSLException, InterruptedException {
        final ScheduledExecutorService metricCollector =
                Executors.newScheduledThreadPool(1);
        try {
            // Configure SSL
            final SslContext sslCtx;
            if(options.isSsl()) {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
            } else {
                sslCtx = null;
            }

            // Configure metrics
            MixServerMetrics metrics = new MixServerMetrics();
            ThroughputCounter throughputCounter =
                    new ThroughputCounter(metricCollector, 5000L, metrics);

            // Register mbean
            if (options.isJmx()) {
                MetricsRegistry.registerMBeans(metrics, options.getPort());
            }

            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.group(bossGroup, workerGroup);
            b.channel(NioServerSocketChannel.class);
            b.handler(new LoggingHandler(LogLevel.INFO));
            b.childHandler(new MixServerInitializer(msgHandler, throughputCounter, sslCtx));

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(options.getPort()).sync();
            this.state = ServerState.RUNNING;
            if (!options.isFork() && options.getWorkerTTLinSec() > 0) {
                long ttl = 1000L * options.getWorkerTTLinSec();
                while (f.channel().isOpen()) {
                    Thread.sleep(1000L);
                    long elapsedTime = System.currentTimeMillis() - msgHandler.getLastHandled();
                    if (elapsedTime > ttl)
                        break;
                }
            } else {
                // Wait until the server socket is closed.
                // In this example, this does not happen, but you can do that to gracefully
                // shut down your server.
                f.channel().closeFuture().sync();
            }
        } finally {
            this.state = ServerState.STOPPING;
            if (options.isJmx()) {
                MetricsRegistry.unregisterMBeans(options.getPort());
            }
            metricCollector.shutdown();
            close();
        }
    }

    @Override
    public void close() {
        // Netty v4.x user guide says "Shutting down a Netty application
        // is usually as simple as shutting down all EventLoopGroups you created
        // via shutdownGracefully(). It returns a Future that notifies you
        // when the EventLoopGroup has been terminated completely and
        // all Channels that belong to the group have been closed.".
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
