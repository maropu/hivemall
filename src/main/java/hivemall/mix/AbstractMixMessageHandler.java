package hivemall.mix;

import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractMixMessageHandler extends SimpleChannelInboundHandler<MixMessage> {
    protected final Log logger = LogFactory.getLog(AbstractMixMessageHandler.class);
}
