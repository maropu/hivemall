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

import hivemall.io.DenseModel;
import hivemall.io.PredictionModel;
import hivemall.io.WeightValue;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.client.AbstractMixClient;
import hivemall.mix.client.MixClient;
import hivemall.mix.client.MixClientEx;
import hivemall.mix.server.MixServer.ServerState;
import hivemall.utils.io.IOUtils;
import hivemall.utils.net.NetUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nonnegative;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MixServerQueryTest {
    private static int NUM_FEATURES_UPDATED = 1000;
    private static int MIN_MIXED_BLOCKED = (int) (NUM_FEATURES_UPDATED * 0.80);
    private static long MAXMUM_WAIT_TIME = 300 * 1000L; // Wait at most 5sec
    private static long NUM_LOOP_QUERY = 500;

    @Ignore
    public void testRemoteQueryClient() throws Exception {
        doExecute("testQueryClient", getRemoteMixServ() + ":" + MixServer.DEFAULT_PORT, 2, false);
    }

    @Ignore
    public void testRemoteQueryClientEx() throws Exception {
        doExecute("testQueryClientEx", getRemoteMixServ() + ":" + MixServer.DEFAULT_PORT, 2, true);
    }

    @Ignore
    public void testQueryClient() throws Exception {
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        try {
            int port = NetUtils.getAvailablePort();
            MixServer server = new MixServer(new String[] {
                    "-port", Integer.toString(port),
                    "-cores", "16",
                    "-sync_threshold", "1"
                });
            serverExec.submit(server);
            waitForState(server, ServerState.RUNNING);
            doExecute("testQueryClient", "localhost:" + port, 2, false);
        } finally {
            serverExec.shutdown();
        }
    }

    @Ignore
    public void testQueryClientEx() throws Exception {
        ExecutorService serverExec = Executors.newSingleThreadExecutor();
        try {
            int port = NetUtils.getAvailablePort();
            MixServer server = new MixServer(new String[] {
                    "-port", Integer.toString(port),
                    "-cores", "16",
                    "-memory", "8192",
                    "-sync_threshold", "1",
                    "-fork"
                });
            serverExec.submit(server);
            waitForState(server, ServerState.RUNNING);
            doExecute("testQueryClientEx", "localhost:" + port, 2, true);
        } finally {
            serverExec.shutdown();
        }
    }

    private void doExecute(String label, String url, int numClients, boolean fork) throws Exception {
        final ExecutorService clientsExec = Executors.newFixedThreadPool(numClients);
        final List<AbstractMixClient> clients = new ArrayList<AbstractMixClient>();
        try {
            final List<PredictionModel> models = new ArrayList<PredictionModel>();
            for (int i = 0; i < numClients; i++) {
                PredictionModel model = new DenseModel(128, false);
                model.configureClock();
                AbstractMixClient client = genMixedClient(url, "MixServerBenchTest" + i, model, fork);
                model.configureMix(client.open(), false);
                clients.add(client);
                models.add(model);
            }
            final Integer[] features = new Integer[NUM_FEATURES_UPDATED];
            final Random rand = new Random(43);
            for (int i = 0; i < features.length; i++) {
                features[i] = rand.nextInt(128);
            }
            Future<?> workers[] = new Future<?>[numClients];
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < numClients; i++) {
                final int index = i;
                workers[i] = clientsExec.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            doQuery(models.get(index), features);
                        } catch (Exception e) {
                            Assert.fail(e.getMessage());
                        }
                    }
                });
            }
            for (Future<?> f : workers) { f.get(); }
            double elapsedTime = (System.currentTimeMillis() - startTime + 0.0) / 1000L;
            double throughput = ((numClients * NUM_LOOP_QUERY * MIN_MIXED_BLOCKED + 0.0) / elapsedTime);
            System.out.println(label + " "
                  + "Troughput: " + throughput + " mixed/s "
                  + "Elapsed: " + elapsedTime + " s");
        } catch (Exception e) {
            throw e;
        } finally {
            clientsExec.shutdown();
            for (AbstractMixClient c : clients) {
                IOUtils.closeQuietly(c);
            }
        }
    }

    private AbstractMixClient genMixedClient(
            String url, String groupId, PredictionModel model,
            boolean fork) {
        if (fork) {
            return new MixClientEx(MixEventName.average, groupId, url, false, 1, model);
        } else {
            return new MixClient(MixEventName.average, groupId, url, false, 1, model);
        }
    }

    private static void doQuery(PredictionModel model, Integer[] features) throws Exception {
        for (int i = 0; i < NUM_LOOP_QUERY; i++) {
            for (Integer feature : features) {
                model.set(feature, new WeightValue(1.f));
            }
            waitForMixed(model, MIN_MIXED_BLOCKED, MAXMUM_WAIT_TIME);
        }
    }

    private static String getRemoteMixServ() {
        final String host = System.getenv("HIVEMALL_TEST_MIXSERV_HOST");
        if (host == null) {
            return "localhost:";
        } else {
            return host;
        }
    }

    private static void waitForState(MixServer server, ServerState expected)
            throws InterruptedException {
        int retry = 0;
        while(server.getState() != expected && retry < 50) {
            Thread.sleep(100);
            retry++;
        }
        Assert.assertEquals("MixServer state is not correct (timed out)", expected, server.getState());
    }

    private static void waitForMixed(
            PredictionModel model, @Nonnegative int minMixed,
            @Nonnegative long maxWaitInMillis)
            throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while(true) {
            int numMixed = model.getNumMixed();
            if(numMixed >= minMixed) {
                break;
            }
            Thread.sleep(100L);
            long elapsedTime = System.currentTimeMillis() - startTime;
            if(elapsedTime > maxWaitInMillis) {
                Assert.fail("Timeout. numMixed = " + numMixed);
            }
        }
    }
}
