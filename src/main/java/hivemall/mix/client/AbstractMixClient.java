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

import java.io.Closeable;
import hivemall.io.ModelUpdateHandler;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.MixedModel;
import hivemall.mix.MixedWeight;
import hivemall.mix.NodeInfo;
import hivemall.utils.hadoop.HadoopUtils;
import io.netty.channel.Channel;

import java.util.HashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public abstract class AbstractMixClient implements ModelUpdateHandler, Closeable {
    public static final String DUMMY_JOB_ID = "__DUMMY_JOB_ID__";

    private final MixEventName event;
    private final String groupID;
    private final int mixThreshold;

    public AbstractMixClient(
            @Nonnull MixEventName event,
            @CheckForNull String groupID,
            int mixThreshold) {
        if(groupID == null) {
            throw new IllegalArgumentException("groupID is null");
        }
        if(mixThreshold < 1 || mixThreshold > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid mixThreshold: " + mixThreshold);
        }
        this.event = event;
        this.groupID = replaceGroupIDIfRequired(groupID);
        this.mixThreshold = mixThreshold;
    }

    /**
     * Initialization, e.g., handling connections to mix servers.
     */
    abstract public MixClient open() throws Exception;

    /**
     * Send a given message to one of mix servers.
     *
     * @param msg contains a parameter for model mixing
     */
    abstract protected void sendMsgToServer(MixMessage msg)
            throws InterruptedException;

    /**
     * @return true if sent request, otherwise false
     */
    @Override
    public boolean onUpdate(
            Object feature, float weight, float covar, short clock, int deltaUpdates) {
        assert(deltaUpdates > 0);
        if(deltaUpdates < mixThreshold) {
            return false; // avoid mixing
        }
        MixMessage msg = new MixMessage(event, feature, weight, covar, clock, deltaUpdates);
        msg.setGroupID(groupID);
        try {
            sendMsgToServer(msg);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    @Override
    public void sendCancelRequest(@Nonnull Object feature, @Nonnull MixedWeight mixed) {
        float weight = mixed.getWeight();
        float covar = mixed.getCovar();
        int deltaUpdates = mixed.getDeltaUpdates();
        MixMessage msg = new MixMessage(event, feature, weight, covar, deltaUpdates, true);
        msg.setGroupID(groupID);
        try {
            sendMsgToServer(msg);
        } catch (InterruptedException e) {
            // Do nothing, but need to log
            // exception messages.
        }
    }

    private static String replaceGroupIDIfRequired(String groupID) {
        if(groupID.startsWith(DUMMY_JOB_ID)) {
            return groupID.replace(DUMMY_JOB_ID, HadoopUtils.getJobId());
        }
        return groupID;
    }
}
