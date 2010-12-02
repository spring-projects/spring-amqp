package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import java.util.concurrent.BlockingQueue;

public class HAQueueingConsumer extends QueueingConsumer {
    public HAQueueingConsumer(HAChannel ch) {
        super(ch);
    }

    public HAQueueingConsumer(HAChannel ch, BlockingQueue<Delivery> q) {
        super(ch, q);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        if(sig.isInitiatedByApplication()){
            super.handleShutdownSignal(consumerTag, sig);
        } //else do nothing...
    }
}
