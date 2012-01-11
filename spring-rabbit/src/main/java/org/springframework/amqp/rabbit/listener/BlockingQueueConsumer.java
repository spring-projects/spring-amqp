/*
 * Copyright 2002-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
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
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;

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

	private final AtomicBoolean cancelReceived = new AtomicBoolean(false);

	private final AcknowledgeMode acknowledgeMode;

	private final ConnectionFactory connectionFactory;

	private final MessagePropertiesConverter messagePropertiesConverter;

	private final ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter;

	private Set<Long> deliveryTags = new LinkedHashSet<Long>();

	/**
	 * Create a consumer. The consumer must not attempt to use the connection factory or communicate with the broker
	 * until it is started.
	 */
	public BlockingQueueConsumer(ConnectionFactory connectionFactory,
			MessagePropertiesConverter messagePropertiesConverter,
			ActiveObjectCounter<BlockingQueueConsumer> activeObjectCounter, AcknowledgeMode acknowledgeMode,
			boolean transactional, int prefetchCount, String... queues) {
		this.connectionFactory = connectionFactory;
		this.messagePropertiesConverter = messagePropertiesConverter;
		this.activeObjectCounter = activeObjectCounter;
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

		MessageProperties messageProperties = this.messagePropertiesConverter.toMessageProperties(
				delivery.getProperties(), envelope, "UTF-8");
		messageProperties.setMessageCount(0);
		Message message = new Message(body, messageProperties);
		if (logger.isDebugEnabled()) {
			logger.debug("Received message: " + message);
		}
		deliveryTags.add(messageProperties.getDeliveryTag());
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
		logger.trace("Retrieving delivery for " + this);
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
		Message message = handle(queue.poll(timeout, TimeUnit.MILLISECONDS));
		if (message == null && cancelReceived.get()) {
			throw new ConsumerCancelledException();
		}
		return message;
	}

	public void start() throws AmqpException {
		if (logger.isDebugEnabled()) {
			logger.debug("Starting consumer " + this);
		}
		this.channel = ConnectionFactoryUtils.getTransactionalResourceHolder(connectionFactory, transactional)
				.getChannel();
		this.consumer = new InternalConsumer(channel);
		this.deliveryTags.clear();
		this.activeObjectCounter.add(this);
		int passiveDeclareTries = 3; // mirrored queue might be being moved
		do {
			try {
				if (!acknowledgeMode.isAutoAck()) {
					// Set basicQos before calling basicConsume (otherwise if we are not acking the broker
					// will send blocks of 100 messages)
					channel.basicQos(prefetchCount);
				}
				for (int i = 0; i < queues.length; i++) {
					channel.queueDeclarePassive(queues[i]);
				}
				passiveDeclareTries = 0;
			} catch (IOException e) {
				if (passiveDeclareTries > 0) {
					if (logger.isWarnEnabled()) {
						logger.warn("Reconnect failed; retries left=" + (passiveDeclareTries-1), e);
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e1) {
							Thread.currentThread().interrupt();
						}
					}
				} else {
					this.activeObjectCounter.release(this);
					throw new FatalListenerStartupException("Cannot prepare queue for listener. "
							+ "Either the queue doesn't exist or the broker will not allow us to use it.", e);
				}
			}
		} while (passiveDeclareTries-- > 0);

		try {
			for (int i = 0; i < queues.length; i++) {
				channel.basicConsume(queues[i], acknowledgeMode.isAutoAck(), consumer);
				if (logger.isDebugEnabled()) {
					logger.debug("Started on queue '" + queues[i] + "': " + this);
				}
			}
		} catch (IOException e) {
			throw RabbitUtils.convertRabbitAccessException(e);
		}
	}

	public void stop() {
		cancelled.set(true);
		if (consumer != null && consumer.getChannel() != null && consumer.getConsumerTag() != null
				&& !this.cancelReceived.get()) {
			try {
				RabbitUtils.closeMessageConsumer(consumer.getChannel(), consumer.getConsumerTag(), transactional);
			} catch (Exception e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Error closing consumer", e);
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Closing Rabbit Channel: " + channel);
		}
		// This one never throws exceptions...
		RabbitUtils.closeChannel(channel);
		deliveryTags.clear();
		consumer = null;
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
			// The delivery tags will be invalid if the channel shuts down
			deliveryTags.clear();
		}

		@Override
		public void handleCancel(String consumerTag) throws IOException {
			if (logger.isWarnEnabled()) {
				logger.warn("Cancel received");
			}
			cancelReceived.set(true);
		}

		@Override
		public void handleCancelOk(String consumerTag) {
			if (logger.isDebugEnabled()) {
				logger.debug("Received cancellation notice for " + BlockingQueueConsumer.this);
			}
			// Signal to the container that we have been cancelled
			activeObjectCounter.release(BlockingQueueConsumer.this);
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
				throws IOException {
			if (cancelled.get()) {
				if (acknowledgeMode.isTransactionAllowed()) {
					return;
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Storing delivery for " + BlockingQueueConsumer.this);
			}
			try {
				// N.B. we can't use a bounded queue and offer() here with a timeout
				// in case the connection thread gets blocked
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

	/**
	 * Perform a rollback, handling rollback exceptions properly.
	 * @param ex the thrown application exception or error
	 * @throws Exception in case of a rollback error
	 */
	public void rollbackOnExceptionIfNecessary(Throwable ex) throws Exception {

		boolean ackRequired = !acknowledgeMode.isAutoAck() && !acknowledgeMode.isManual();
		try {
			if (transactional) {
				if (logger.isDebugEnabled()) {
					logger.debug("Initiating transaction rollback on application exception: " + ex);
				}
				RabbitUtils.rollbackIfNecessary(channel);
			}
			if (ackRequired) {
				if (logger.isDebugEnabled()) {
					logger.debug("Rejecting messages");
				}
				for (Long deliveryTag : deliveryTags) {
					// With newer RabbitMQ brokers could use basicNack here...
					channel.basicReject(deliveryTag, true);
				}
				if (transactional) {
					// Need to commit the reject (=nack)
					RabbitUtils.commitIfNecessary(channel);
				}
			}
		} catch (Exception e) {
			logger.error("Application exception overridden by rollback exception", ex);
			throw e;
		} finally {
			deliveryTags.clear();
		}
	}

	/**
	 * Perform a commit or message acknowledgement, as appropriate.
	 * @param locallyTransacted
	 * @throws IOException
	 */
	public boolean commitIfNecessary(boolean locallyTransacted) throws IOException {

		if (deliveryTags.isEmpty()) {
			return false;
		}

		try {

			boolean ackRequired = !acknowledgeMode.isAutoAck() && !acknowledgeMode.isManual();

			if (ackRequired) {

				if (transactional && !locallyTransacted) {

					// Not locally transacted but it is transacted so it
					// could be synchronized with an external transaction
					for (Long deliveryTag : deliveryTags) {
						ConnectionFactoryUtils.registerDeliveryTag(connectionFactory, channel, deliveryTag);
					}

				} else {

					if (!deliveryTags.isEmpty()) {
						long deliveryTag = new ArrayList<Long>(deliveryTags).get(deliveryTags.size() - 1);
						channel.basicAck(deliveryTag, true);
					}

				}
			}

			if (locallyTransacted) {
				// For manual acks we still need to commit
				RabbitUtils.commitIfNecessary(channel);
			}

		} finally {
			deliveryTags.clear();
		}

		return true;

	}

}