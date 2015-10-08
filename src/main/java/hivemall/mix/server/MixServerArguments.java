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

import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.lang.Primitives;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public final class MixServerArguments {

    private final int port;
    private final int cores;
    private final int memoryMb;
    private final boolean ssl;
    private final float scale;
    private final short syncThreshold;
    private final long sessionTTLinSec;
    private final long workerTTLinSec;
    private final long sweepSessionIntervalInSec;
    private final long sweepWorkerIntervalInSec;
    private final boolean fork;
    private final boolean jmx;

    public MixServerArguments(String args[]) {
        Options opts = getOptions();
        CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        this.port = Primitives.parseInt(cl.getOptionValue("port"), MixServer.DEFAULT_PORT);
        this.ssl = cl.hasOption("ssl");
        this.cores = Primitives.parseInt(cl.getOptionValue("cores"),
                Runtime.getRuntime().availableProcessors());
        this.memoryMb = Primitives.parseInt(cl.getOptionValue("memory"),
                (int) Runtime.getRuntime().maxMemory() / (1024 * 1024));
        this.scale = Primitives.parseFloat(cl.getOptionValue("scale"), 1.f);
        this.syncThreshold = Primitives.parseShort(cl.getOptionValue("sync"), (short) 30);
        this.sessionTTLinSec = Primitives.parseLong(cl.getOptionValue("sttl"), 120L);
        this.workerTTLinSec = Primitives.parseLong(cl.getOptionValue("wttl"), 0L);
        this.sweepSessionIntervalInSec = Primitives.parseLong(cl.getOptionValue("sweep"), 60L);
        this.sweepWorkerIntervalInSec = Primitives.parseLong(cl.getOptionValue("sweep"), 120L);
        this.fork = cl.hasOption("fork");
        this.jmx = cl.hasOption("jmx");
    }

    private static Options getOptions() {
        Options opts = new Options();
        opts.addOption("p", "port", true, "Port number of the mix server [default: 11212]");
        opts.addOption("ssl", false, "Use SSL for the mix communication [default: false]");
        opts.addOption("c", "cores", true, "Total CPU cores to allow threads for requests [default: Runtime#availableProcessors()]");
        opts.addOption("m", "memory", true, "Total amount of megabyte memory to use for requests [default: Runtime#maxMemory()]");
        opts.addOption("scale", "scalemodel", true, "Scale values of prediction models to avoid overflow [default: 1.0 (no-scale)]");
        opts.addOption("sync", "sync_threshold", true, "Synchronization threshold using clock difference [default: 30]");
        opts.addOption("sttl", "session_ttl", true, "The TTL in sec that an idle session lives [default: 120 sec]");
        opts.addOption("wttl", "worker_ttl", true, "The TTL in sec that an idle worker lives [default: 0 sec]");
        opts.addOption("sweep", "session_sweep_interval", true, "The interval in sec that the session expiry thread runs [default: 60 sec]");
        opts.addOption("fork", "fork_jvm", false, "Fork JVM processes for requests [default: false]");
        opts.addOption("jmx", "metrics", false, "Toggle this option to enable monitoring metrics using JMX [default: false]");
        return opts;
    }

    public int getPort() {
        return port;
    }

    public int getCores() {
        return cores;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public boolean isSsl() {
        return ssl;
    }

    public float getScale() {
        return scale;
    }

    public short getSyncThreshold() {
        return syncThreshold;
    }

    public long getSessionTTLinSec() {
        return sessionTTLinSec;
    }

    public long getWorkerTTLinSec() {
        return workerTTLinSec;
    }

    public long getSweepSessionIntervalInSec() {
        return sweepSessionIntervalInSec;
    }

    public long getSweepWorkerIntervalInSec() {
        return sweepWorkerIntervalInSec;
    }

    public boolean isFork() {
        return fork;
    }

    public boolean isJmx() {
        return jmx;
    }
}
