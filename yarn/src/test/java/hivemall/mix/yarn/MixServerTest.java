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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

public class MixServerTest {

    private static final Log logger = LogFactory.getLog(ApplicationMaster.class);
    private static final String appMasterJar = JarFinder.getJar(ApplicationMaster.class);
    private static final int numNodeManager = 2;

    private MiniYARNCluster yarnCluster;
    private YarnConfiguration conf;

    @Before
    public void setup() throws Exception {
        logger.info("Starting up a YARN cluster");

        conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.set("yarn.log.dir", "target");
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
        conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
        conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

        if (yarnCluster == null) {
            yarnCluster = new MiniYARNCluster(
                    MixServerTest.class.getSimpleName(), 1, numNodeManager, 1, 1);
            yarnCluster.init(conf);
            yarnCluster.start();

            waitForNMsToRegister();

            final URL url = this.getClass().getResource("/yarn-site.xml");
            Assert.assertNotNull("Could not find 'yarn-site.xml' dummy file in classpath", url);

            Configuration yarnClusterConfig = yarnCluster.getConfig();
            yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            OutputStream os = new FileOutputStream(new File(url.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
        }
    }

    @After
    public void tearDown() throws IOException {
        if (yarnCluster != null) {
            try {
                yarnCluster.stop();
            } finally {
                yarnCluster = null;
            }
        }
    }

    private void waitForNMsToRegister() throws Exception {
        int retry = 0;
        while (true) {
            Thread.sleep(1000L);
            if (yarnCluster.getResourceManager().getRMContext().getRMNodes().size()
                    >= numNodeManager) {
                break;
            }
            if (retry++ > 60) {
                Assert.fail("Can't launch a yarn cluster");
            }
        }
    }

    @Test(timeout=90000)
    public void testSimpleScenario() throws Exception {
        final String[] args = {
            "--jar", appMasterJar,
            "--num_containers", "1",
            "--master_memory", "128",
            "--master_vcores", "1",
            "--container_memory", "512",
            "--container_vcores", "1"
        };

        final MixServerRunner runner = new MixServerRunner(new Configuration(yarnCluster.getConfig()));
        boolean initSuccess = runner.init(args);
        Assert.assertTrue(initSuccess);

        final AtomicBoolean result = new AtomicBoolean(false);
        Thread mixCluster = new Thread() {

            @Override
            public void run() {
                try {
                    result.set(runner.run());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        mixCluster.start();

        // TODO: Do some tests...

        mixCluster.join();
    }
}
