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
package hivemall.mix.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class ApplicationMaster {

    private static final Log logger = LogFactory.getLog(ApplicationMaster.class);

    private final String containerMainClass;
    private final Options opts;
    private final Configuration conf;

    // Application Attempt Id (combination of attemptId and fail count)
    private ApplicationAttemptId appAttemptID;

    // Variables passed from MixServerRunner
    private String sharedDir;
    private String mixServJar;

    // Handle to communicate with RM/NM
    private AMRMClientAsync amRMClientAsync;
    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;

    // Resource parameters for containers
    private int containerMemory;
    private int containerVCores;
    private int numContainers;
    private int requestPriority;
    private int numRetryForFailedConainers;

    // Launched threads
    private final List<Thread> launchThreads = new ArrayList<Thread>();

    private final Set<ContainerId> launchedContainers =
            Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());
    private final AtomicInteger numAllocatedContainers = new AtomicInteger();
    private final AtomicInteger numRequestedContainers = new AtomicInteger();
    private final AtomicInteger numCompletedContainers = new AtomicInteger();
    private final AtomicInteger numFailedContainers = new AtomicInteger();

    private volatile boolean isFinished = false;

    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            logger.fatal("Error running AM", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            logger.info("AM completed successfully");
            System.exit(0);
        } else {
            logger.info("AM failed");
            System.exit(2);
        }
    }

    public ApplicationMaster() {
        this.containerMainClass = "hivemall.mix.server.MixServer";
        this.conf = new YarnConfiguration();
        this.opts = new Options();
        opts.addOption("num_containers", true, "# of containers for mix servers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run a mix server");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run a mix server");
        opts.addOption("priority", true, "Application Priority [Default: 0]");
        opts.addOption("num_retries", true, "# of retries for failed containers [Default: 32]");
        opts.addOption("help", false, "Print usage");
    }

    // Helper function to print out usage
    private void printUsage() {
        new HelpFormatter().printHelp("ApplicatonMaster", opts);
    }

    public boolean init(String[] args) throws ParseException, IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "No args specified for MixServerRunner to initialize");
        }

        CommandLine cliParser = new GnuParser().parse(opts, args);
        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        // Get variables from envs
        appAttemptID = ConverterUtils.toContainerId(getEnv(Environment.CONTAINER_ID.name()))
                .getApplicationAttemptId();
        sharedDir = getEnv(YarnUtils.MIXSERVER_RESOURCE_LOCATION);
        mixServJar = getEnv(YarnUtils.MIXSERVER_CONTAINER_APP);

        // Get variables from arguments
        containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        numRetryForFailedConainers = Integer.parseInt(cliParser.getOptionValue("num_retries", "32"));
        if (numContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }

        try {
            Log4jPropertyHelper.updateLog4jConfiguration(
                    ApplicationMaster.class, "log4j.properties");
        } catch (Exception e) {
            logger.warn("Can not set up custom log4j properties. " + e);
        }

        logger.info("Application master for "
                + "appId:" + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp:" + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId:" + appAttemptID.getAttemptId()
                + ", containerVCores:" + containerVCores
                + ", containerMemory:" + containerMemory
                + ", numContainers:" + numContainers
                + ", requestPriority:" + requestPriority);

        return true;
    }

    private String getEnv(String key) {
        final String value = System.getenv(key);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(key + "not set in the environment");
        }
        return value;
    }

    public void run() throws YarnException, IOException, InterruptedException {
        // AM <--> RM
        amRMClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
        amRMClientAsync.init(conf);
        amRMClientAsync.start();

        // AM <--> NM
        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Register self with ResourceManager to start
        // heartbeating to the RM.
        RegisterApplicationMasterResponse response = amRMClientAsync
            .registerApplicationMaster("", -1, "");

        // A resource ask cannot exceed the max
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        if (containerVCores > maxVCores) {
            logger.warn("cores:" + containerVCores + " requested, but only cores:"
                    + maxVCores + " available.");
            containerVCores = maxVCores;
        }
        int maxMem = response.getMaximumResourceCapability().getMemory();
        if (containerMemory > maxMem) {
            logger.warn("mem:" + containerMemory + " requested, but only mem:"
                    + maxMem + " available.");
            containerMemory = maxMem;
        }

        for (int i = 0; i < numContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClientAsync.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numContainers);
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        private int retryRequest = 0;

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            logger.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                logger.info(appAttemptID + " got container status for "
                        + "containerID:" + containerStatus.getContainerId()
                        + ", state:" + containerStatus.getState()
                        + ", exitStatus:" + containerStatus.getExitStatus()
                        + ", diagnostics:" + containerStatus.getDiagnostics());

                // Non complete containers should not be here
                assert containerStatus.getState() == ContainerState.COMPLETE;

                // Ignore containers we know nothing about - probably
                // from a previous attempt.
                if (!launchedContainers.contains(containerStatus.getContainerId())) {
                    logger.info("Ignoring completed status of "
                            + containerStatus.getContainerId()
                            + "; unknown container (probably launched by previous attempt)");
                    continue;
                }

                // Increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (exitStatus != 0) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                }
            }

            // Ask for more containers if any failed
            int askCount = numContainers - numRequestedContainers.get();
            if (retryRequest++ < numRetryForFailedConainers && askCount > 0) {
                logger.info("Retry " + askCount + " requests for failed containers");
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClientAsync.addContainerRequest(containerAsk);
                }
                numRequestedContainers.addAndGet(askCount);
            }

            if (numCompletedContainers.get() == numContainers) {
                isFinished = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            logger.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                logger.info("Launching a mix server on a new container: "
                        + "containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                            + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory=" + allocatedContainer.getResource().getMemory()
                        + ", containerResourceVirtualCores="
                            + allocatedContainer.getResource().getVirtualCores());

                // Launch and start the container on a separate thread to keep
                // the main thread unblocked as all containers
                // may not be allocated at one go.
                Thread launchThread = createLaunchContainerThread(allocatedContainer);
                launchThreads.add(launchThread);
                launchedContainers.add(allocatedContainer.getId());
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            isFinished = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {}

        @Override
        public float getProgress() {
            // Set progress to deliver to RM on next heartbeat
            return (float) numCompletedContainers.get() / numContainers;
        }

        @Override
        public void onError(Throwable throwable) {
            isFinished = true;
            amRMClientAsync.stop();
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private final ApplicationMaster applicationMaster;

        private ConcurrentMap<ContainerId, Container> containers;

        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
            this.containers = new ConcurrentHashMap<ContainerId, Container>();
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            if (logger.isDebugEnabled()) {
                logger.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(
                        containerId, container.getNodeId());
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus status) {
            if (logger.isDebugEnabled()) {
                logger.debug("Container Status: id=" + containerId + ", status=" + status);
            }
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (logger.isDebugEnabled()) {
                logger.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    private boolean finish() throws InterruptedException {
        while (!isFinished && (numCompletedContainers.get() != numContainers)) {
            Thread.sleep(60 * 1000L);
        }

        // Join all launched threads
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                logger.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
          }
        }

        // When the application completes, it should stop all
        // running containers.
        nmClientAsync.stop();

        // When the application completes, it should send a finish
        // application signal to the RM.
        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics: "
                    + "total=" + numContainers
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            logger.info(appMessage);
            success = false;
        }

        try {
            amRMClientAsync.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (Exception e) {
            logger.error("Failed to unregister application", e);
        }

        amRMClientAsync.stop();

        return success;
    }

    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Priority.newInstance(requestPriority);
        Resource capability = Resource.newInstance(containerMemory, containerVCores);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        logger.info("Requested container ask: " + request.toString());
        return request;
    }

    private Thread createLaunchContainerThread(Container allocatedContainer) {
        return new Thread(new LaunchContainerRunnable(allocatedContainer, containerListener));
    }

    // Thread to launch the container that will execute a mix server
    private class LaunchContainerRunnable implements Runnable {

        private final Container container;
        private final NMCallbackHandler containerListener;

        public LaunchContainerRunnable(Container container, NMCallbackHandler containerListener) {
            this.container = container;
            this.containerListener = containerListener;
        }

        @Override
        public void run() {
            // Set local resources (e.g., local files or archives)
            // for the allocated container.
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            try {
                final FileSystem fs = FileSystem.get(conf);
                final Path mixServJarDst = new Path(sharedDir, mixServJar);
                localResources.put(mixServJarDst.getName(),
                        YarnUtils.createLocalResource(fs, mixServJarDst));
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Set the env variables to be setup in the env
            // where the container will be run.
            Map<String, String> env = new HashMap<String, String>();
            {
                StringBuilder classPathEnv = new StringBuilder();
                YarnUtils.addClassPath(Environment.CLASSPATH.$$(), classPathEnv);
                YarnUtils.addClassPath("./*", classPathEnv);
                YarnUtils.addClassPath("./log4j.properties", classPathEnv);
                YarnUtils.addClassPath(System.getProperty("java.class.path"), classPathEnv);
                env.put("CLASSPATH", classPathEnv.toString());
            }

            // Set the necessary command to execute AM
            List<String> commands = new ArrayList<String>();
            {
                Vector<CharSequence> vargs = new Vector<CharSequence>(30);

                vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
                vargs.add("-Xms" + containerMemory + "m");
                vargs.add("-Xmx" + containerMemory + "m");
                vargs.add(containerMainClass);
                vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
                vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

                StringBuilder command = new StringBuilder();
                for (CharSequence str : vargs) {
                    command.append(str).append(" ");
                }

                final String commandStr = command.toString();
                commands.add(commandStr);
                logger.info("Executed command for the container: " + commandStr);
            }

            // Launch container by ContainerLaunchContext
            ContainerLaunchContext ctx = ContainerLaunchContext
                    .newInstance(localResources, env, commands, null, null, null);
            nmClientAsync.startContainerAsync(container, ctx);

            containerListener.addContainer(container.getId(), container);
        }
    }
}
