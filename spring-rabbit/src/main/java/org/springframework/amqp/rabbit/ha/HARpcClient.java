package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.RpcClient;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.BlockingCell;

import java.io.IOException;
import java.util.Map;

public class HARpcClient extends RpcClient {

    private HAChannelConfiguration channelConfiguration;


    /**
     * Construct a new RpcClient that will communicate on the given channel, sending
     * requests to the given exchange with the given routing key.
     * <p/>
     * Causes the creation of a temporary private autodelete queue.
     *
     * @param channel    the channel to use for communication
     * @param exchange   the exchange to connect to
     * @param routingKey the routing key
     * @throws java.io.IOException if an error is encountered
     * @see #setupReplyQueue
     */
    public HARpcClient(HAChannel channel, String exchange, String routingKey) throws IOException {
        super(channel, exchange, routingKey);
    }

    @Override
    protected DefaultConsumer setupConsumer() throws IOException {

        final DefaultConsumer consumer = new DefaultConsumer(getChannel()) {
            @Override
            public void handleShutdownSignal(String consumerTag,
                                             ShutdownSignalException signal) {
                if(signal.isInitiatedByApplication()){
                    synchronized (getContinuationMap()) {
                        for (Map.Entry<String, BlockingCell<Object>> entry : getContinuationMap().entrySet()) {
                            entry.getValue().set(signal);
                        }
                        //_consumer = null; TODO need to confirm that this omission will not cause a problem.
                    }
                } //else do nothing and let shutdown continue.
            }

            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                synchronized (getContinuationMap()) {
                    String replyId = properties.getCorrelationId();
                    BlockingCell<Object> blocker = getContinuationMap().get(replyId);
                    getContinuationMap().remove(replyId);
                    blocker.set(body);
                }
            }
        };

        final HAChannel channel = getChannel();
        channelConfiguration = new HAChannelConfiguration(){
            public void configureChannel(Channel channel) throws IOException {
                channel.basicConsume(getReplyQueue(), true, consumer);
            }
        };
        channel.addInternalConfiguration(channelConfiguration);
        return consumer;
    }

    @Override
    public void close() throws IOException {
        if(channelConfiguration != null){
            getChannel().removeInternalConfiguration(channelConfiguration);
            channelConfiguration = null;
        }
        super.close();
    }

    @Override
    public HAChannel getChannel() {
        return (HAChannel) super.getChannel();
    }
}
