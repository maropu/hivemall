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
import hivemall.mix.store.PartialResult;
import hivemall.mix.store.SessionObject;
import hivemall.mix.store.SessionStore;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

@Sharable
public final class MixWorkerHandler extends BaseMixHandler {

    public MixWorkerHandler(
            @Nonnull SessionStore sessionStore,
            @Nonnegative int syncThreshold,
            @Nonnegative float scale) {
        super(sessionStore, syncThreshold, scale);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MixMessage msg) throws Exception {
        setLastHandled(System.currentTimeMillis());
        final MixEventName event = msg.getEvent();
        switch(event) {
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
            case ping: {
                // Respond to let ForkWorkerHandler know if this mix worker
                // starts handling mix requests.
                ctx.writeAndFlush(new MixMessage(MixEventName.ack));
                break;
            }
            case killWorker: {
                // Close this handler, then parent one
                ctx.channel().close();
                ctx.channel().parent().close();
                break;
            }
            default:
                throw new IllegalStateException("Unexpected event: " + event);
        }
    }
}
