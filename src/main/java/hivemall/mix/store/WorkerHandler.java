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

import hivemall.mix.launcher.WorkerProcessBuilder;
import hivemall.mix.server.MixServer;
import hivemall.mix.server.MixServer.ServerState;
import hivemall.mix.server.MixServerArguments;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public final class WorkerHandler implements Closeable {

    private final MixServerArguments options;
    private final ConcurrentMap<String, WorkerObject> workers;
    private final ExecutorService workerExec;
    private final EventLoopGroup workerGroup;
    private final int totalCores;
    private final int totalMemoryMb;

    private AtomicInteger coresUsed;
    private AtomicInteger memoryMbUsed;

    public WorkerHandler(MixServerArguments options, int totalCores, int totalMemoryMb) {
        this.options = options;
        this.workers = new ConcurrentHashMap<String, WorkerObject>();
        this.workerExec = Executors.newCachedThreadPool();
        this.workerGroup = new NioEventLoopGroup(1);
        this.totalCores = totalCores;
        this.totalMemoryMb = totalMemoryMb;
        this.coresUsed = new AtomicInteger(0);
        this.memoryMbUsed = new AtomicInteger(0);
    }

    public synchronized WorkerObject get(String groupID, int cores, int memoryMb)
            throws IllegalStateException, InterruptedException {
        assert !workerExec.isShutdown();
        WorkerObject worker = workers.get(groupID);
        if (worker == null) {
            int coresFree = totalCores - coresUsed.get();
            int memoryMbFree = totalMemoryMb - memoryMbUsed.get();
            if (coresFree >= cores && memoryMbFree >= memoryMb) {
                coresUsed.addAndGet(cores);
                memoryMbUsed.addAndGet(memoryMb);
                worker = new WorkerObject(workerGroup, cores, memoryMb, coresUsed, memoryMbUsed);
                workerExec.submit(new WorkerRunner(options, worker));
                workers.put(groupID, worker);
            } else {
                throw new IllegalStateException(
                        cores + "cores " + memoryMb + "MB requested, but "
                      + coresFree + "cores " + memoryMbFree + "MB only available");
            }
        }
        return worker;
    }

    @Override
    public void close() {
        // First, shutdown workerExec to stop
        // running new processes.
        workerExec.shutdown();

        // Then, close all the workers that it holds
        final Set<Map.Entry<String, WorkerObject>> entries = workers.entrySet();
        for (Map.Entry<String, WorkerObject> e : entries) {
            WorkerObject worker = e.getValue();
            worker.close();
        }
        workerGroup.shutdownGracefully();
    }

    @ThreadSafe
    private static final class WorkerRunner implements Runnable {

        private final MixServerArguments options;
        private final WorkerObject self;

        public WorkerRunner(
                @Nonnull MixServerArguments options,
                @Nonnull WorkerObject self) {
            this.options = options;
            this.self = self;
        }

        @Override
        public void run() {
            List<String> arguments = new ArrayList<String>();
            arguments.addAll(
                    Arrays.asList(
                            "-port", Integer.toString(self.getPort()),
                            "-cores", Integer.toString(self.getCores()),
                            "-scalemodel", Float.toString(options.getScale()),
                            "-sync_threshold", Short.toString(options.getSyncThreshold()),
                            "-session_ttl", Long.toString(options.getSessionTTLinSec()),
                            "-worker_ttl", "180",
                            "-session_sweep_interval",
                            Long.toString(options.getSweepSessionIntervalInSec())
                    )
            );
            if (options.isSsl()) arguments.add("-ssl");
            if (options.isJmx()) arguments.add("-metrics");
            WorkerProcessBuilder builder =
                    new WorkerProcessBuilder(MixServer.class, self.getMemoryMb(), arguments, null);
            try {
                // Block until the process ends
                builder.execute().waitFor();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                self.close();
            }
        }
    }

    @ThreadSafe
    public static final class StoppedWorkerSweeper implements Runnable {

        private final ConcurrentMap<String, WorkerObject> workers;

        public StoppedWorkerSweeper(@Nonnull WorkerHandler handler) {
            this.workers = handler.workers;
        }

        @Override
        public void run() {
            final Set<Map.Entry<String, WorkerObject>> entries = workers.entrySet();
            final Iterator<Map.Entry<String, WorkerObject>> itor = entries.iterator();
            while (itor.hasNext()) {
                Map.Entry<String, WorkerObject> e = itor.next();
                WorkerObject worker = e.getValue();
                if (worker.getState() == ServerState.STOPPING) {
                    itor.remove();
                }
            }
        }
    }
}
