package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.RpcServer;

import java.io.IOException;

/**
 * HA implementation of the RPC server that reattaches its consumer once a failure has been detected.
 */
public class HARpcServer extends RpcServer {
    private HAChannelConfiguration channelConfiguration;

    public HARpcServer(HAChannel channel) throws IOException {
        super(channel);
    }

    public HARpcServer(HAChannel channel, String queueName) throws IOException {
        super(channel, queueName);
    }

    @Override
    protected QueueingConsumer setupConsumer() throws IOException {
        final QueueingConsumer consumer = new HAQueueingConsumer(getHAChannel());
        channelConfiguration = new HAChannelConfiguration(){
            public void configureChannel(Channel channel) throws IOException {
                channel.basicConsume(_queueName, consumer);
            }
        };
        getHAChannel().addInternalConfiguration(channelConfiguration);
        return consumer;
    }

    @Override
    public void close() throws IOException {
        if(channelConfiguration != null){
            getHAChannel().removeInternalConfiguration(channelConfiguration);
            channelConfiguration = null;
        }
        super.close();

    }

    private HAChannel getHAChannel() {
        return ((HAChannel) _channel);
    }
}
