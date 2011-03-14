package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.support.RabbitUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;

/**
 * Specialized consumer encapsulating knowledge of the broker connections and having its own lifecycle (start and stop).
 * 
 * @author Mark Pollack
 * @author Dave Syer
 * 
 */
public class BlockingQueueConsumer {

	private static Log logger = LogFactory.getLog(BlockingQueueConsumer.class);

	// This must be an unbounded queue or we risk blocking the Connection thread.
	private final BlockingQueue<Delivery> queue = new LinkedBlockingQueue<Delivery>();

	// When this is non-null the connection has been closed (should never happen in normal operation).
	private volatile ShutdownSignalException shutdown;

	private final String[] queues;

	private final int prefetchCount;

	private final boolean transactional;

	private Channel channel;

	private InternalConsumer consumer;

	private final AtomicBoolean cancelled = new AtomicBoolean(false);

	private final AcknowledgeMode acknowledgeMode;

	private final ConnectionFactory connectionFactory;

	/**
	 * Create a consumer. The consumer must not attempt to use the connection factory or communicate with the broker
	 * until it is started.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, String... queues) {
		this.connectionFactory = connectionFactory;
		this.acknowledgeMode = acknowledgeMode;
		this.transactional = transactional;
		this.prefetchCount = prefetchCount;
		this.queues = queues;
	}

	public Channel getChannel() {
		return channel;
	}

	public String getConsumerTag() {
		return consumer.getConsumerTag();
	}

	/**
	 * Check if we are in shutdown mode and if so throw an exception.
	 */
	private void checkShutdown() {
		if (shutdown != null) {
			throw Utility.fixStackTrace(shutdown);
		}
	}

	/**
	 * If this is a non-POISON non-null delivery simply return it. If this is POISON we are in shutdown mode, throw
	 * shutdown. If delivery is null, we may be in shutdown mode. Check and see.
	 * 
	 * @throws InterruptedException
	 */
	private Message handle(Delivery delivery) throws InterruptedException {
		if ((delivery == null && shutdown != null)) {
			throw shutdown;
		}
		if (delivery == null) {
			return null;
		}
		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();

		MessageProperties messageProperties = RabbitUtils.createMessageProperties(delivery.getProperties(), envelope,
				"UTF-8");
		messageProperties.setMessageCount(0);
		Message message = new Message(body, messageProperties);
		if (logger.isDebugEnabled()) {
			logger.debug("Received message: " + message);
		}
		return message;
	}

	/**
	 * Main application-side API: wait for the next message delivery and return it.
	 * 
	 * @return the next message
	 * @throws InterruptedException if an interrupt is received while waiting
	 * @throws ShutdownSignalException if the connection is shut down while waiting
	 */
	public Message nextMessage() throws InterruptedException, ShutdownSignalException {
		logger.debug("Retrieving delivery for " + this);
		return handle(queue.take());
	}

	/**
	 * Main application-side API: wait for the next message delivery and return it.
	 * 
	 * @param timeout timeout in millisecond
	 * @return the next message or null if timed out
	 * @throws InterruptedException if an interrupt is received while waiting
	 * @throws ShutdownSignalException if the connection is shut down while waiting
	 */
	public Message nextMessage(long timeout) throws InterruptedException, ShutdownSignalException {
		if (logger.isDebugEnabled()) {
			logger.debug("Retrieving delivery for " + this);
		}
		checkShutdown();
		return handle(queue.poll(timeout, TimeUnit.MILLISECONDS));
	}

	public void start() throws AmqpException {
		this.channel = ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, transactional)
				.getChannel();
		this.consumer = new InternalConsumer(channel);
		try {
			// Set basicQos before calling basicConsume (it is ignored if we are not transactional and the broker will
			// send blocks of 100 messages)
			channel.basicQos(prefetchCount);
			for (int i = 0; i < queues.length; i++) {
				channel.queueDeclarePassive(queues[i]);
				channel.basicConsume(queues[i], acknowledgeMode.isAutoAck(), consumer);
				if (logger.isDebugEnabled()) {
					logger.debug("Started " + this);
				}
			}
		} catch (IOException e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

	public void stop() {
		cancelled.set(true);
		logger.debug("Closing Rabbit Channel: " + channel);
		if (consumer != null && consumer.getChannel() != null && consumer.getConsumerTag() != null) {
			RabbitUtils.closeMessageConsumer(consumer.getChannel(), consumer.getConsumerTag(), transactional);
		}
		// This one never throws exceptions...
		RabbitUtils.closeChannel(channel);
	}

	private class InternalConsumer extends DefaultConsumer {

		public InternalConsumer(Channel channel) {
			super(channel);
		}

		@Override
		public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
			if (logger.isDebugEnabled()) {
				logger.debug("Received shutdown signal for consumer tag=" + consumerTag, sig);
			}
			shutdown = sig;
			// TODO: interrupt?
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
				throws IOException {
			if (cancelled.get()) {
				if (acknowledgeMode.isTransactionAllowed()) {
					return;
				}
			}
			// TODO: do we want to pass on 'consumerTag'?
			logger.debug("Storing delivery for " + BlockingQueueConsumer.this);
			try {
				// TODO: If transactional we could use a bounded queue and offer() here with a timeout
				// in which case if it fails we could nack the message and have it requeued.
				queue.put(new Delivery(envelope, properties, body));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

	}

	/**
	 * Encapsulates an arbitrary message - simple "bean" holder structure.
	 */
	private static class Delivery {

		private final Envelope envelope;
		private final AMQP.BasicProperties properties;
		private final byte[] body;

		public Delivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
			this.envelope = envelope;
			this.properties = properties;
			this.body = body;
		}

		public Envelope getEnvelope() {
			return envelope;
		}

		public BasicProperties getProperties() {
			return properties;
		}

		public byte[] getBody() {
			return body;
		}
	}

	@Override
	public String toString() {
		return "Consumer: tag=[" + (consumer != null ? consumer.getConsumerTag() : null) + "], channel=" + channel
				+ ", acknowledgeMode=" + acknowledgeMode + " local queue size=" + queue.size();
	}

}