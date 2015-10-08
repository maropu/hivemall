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

import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.store.*;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

@ChannelHandler.Sharable
public final class ForkWorkerHandler extends BaseMixHandler {

    static MixMessage FORK_FAILURE_MESSAGE =
            new MixMessage(MixEventName.forkWorker, -1);

    private final WorkerHandler handler;

    public ForkWorkerHandler(
            @Nonnull WorkerHandler handler,
            @Nonnull SessionStore sessionStore,
            @Nonnegative int syncThreshold,
            @Nonnegative float scale) {
        super(sessionStore, syncThreshold, scale);
        this.handler = handler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        final MixEventName event = msg.getEvent();
        switch(event) {
            case forkWorker: {
                MixMessage responseMsg = FORK_FAILURE_MESSAGE;
                final String groupID = msg.getGroupID();
                final int cores = msg.getCores();
                final int memoryMb = msg.getMemoryMb();
                if (groupID == null) {
                    throw new IllegalStateException("JobID is not set in the request message");
                }
                try {
                    WorkerObject worker = handler.get(groupID, cores, memoryMb);
                    worker.waitForRunning();
                    responseMsg = new MixMessage(MixEventName.forkWorker, worker.getPort());
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                }
                ctx.writeAndFlush(responseMsg).sync();
                break;
            }

            // TODO: The Initial implementation handles model mixing
            // because of backward compatibility.
            // We need to remove the functionality in a future release.
            case average:
            case argminKLD: {
                SessionObject session = getSession(msg);
                PartialResult partial = getPartialResult(msg, session);
                mix(ctx, msg, partial, session);
                break;
            }
            case closeGroup: {
                closeGroup(msg);
                break;
            }
            default:
                logger.warn("Unexpected event: " + event);
        }
    }
}
