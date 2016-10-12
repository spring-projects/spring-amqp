/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;

/**
 * Listener container for Direct ReplyTo only listens to the pseudo queue
 * {@link Address#AMQ_RABBITMQ_REPLY_TO}. Consumers are added on-demand and
 * terminated when idle for {@link #setIdleEventInterval(long) idleEventInterval}
 * (default 60 seconds).
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DirectReplyToMessageListenerContainer extends DirectMessageListenerContainer {

	private static final int DEFAULT_IDLE = 60000;

	private final ConcurrentMap<Channel, SimpleConsumer> inUseConsumerChannels = new ConcurrentHashMap<>();

	private final ConcurrentMap<SimpleConsumer, Long> whenUsed = new ConcurrentHashMap<>();

	private int consumerCount;

	public DirectReplyToMessageListenerContainer(ConnectionFactory connectionFactory) {
		super(connectionFactory);
		super.setQueueNames(Address.AMQ_RABBITMQ_REPLY_TO);
		setAcknowledgeMode(AcknowledgeMode.NONE);
		super.setConsumersPerQueue(0);
		super.setIdleEventInterval(DEFAULT_IDLE);
	}

	@Override
	public final void setConsumersPerQueue(int consumersPerQueue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void setMonitorInterval(long monitorInterval) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void setQueueNames(String... queueName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void setQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void addQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void addQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final boolean removeQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final boolean removeQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMessageListener(Object messageListener) {
		throw new UnsupportedOperationException(
				"'messageListener' must be a 'MessageListener' or 'ChannelAwareMessageListener'");
	}

	@Override
	public void setChannelAwareMessageListener(ChannelAwareMessageListener messageListener) {
		super.setChannelAwareMessageListener((message, channel) -> {
			this.inUseConsumerChannels.remove(channel);
			messageListener.onMessage(message, channel);
		});
	}

	@Override
	public void setMessageListener(MessageListener messageListener) {
		super.setChannelAwareMessageListener((message, channel) -> {
			this.inUseConsumerChannels.remove(channel);
			messageListener.onMessage(message);
		});
	}

	@Override
	protected void doStart() throws Exception {
		if (!isRunning()) {
			this.consumerCount = 0;
			super.setConsumersPerQueue(0);
			super.doStart();
		}
	}

	@Override
	protected void processMonitorTask() {
		long now = System.currentTimeMillis();
		synchronized (this.consumersMonitor) {
			long reduce = this.consumers.stream()
				.filter(c -> this.whenUsed.containsKey(c) && !this.inUseConsumerChannels.containsValue(c)
						&& this.whenUsed.get(c) < now - getIdleEventInterval())
				.count();
			if (reduce > 0) {
				if (logger.isDebugEnabled()) {
					logger.debug("Reducing idle consumes by " + reduce);
				}
				this.consumerCount = (int) Math.max(0, this.consumerCount - reduce);
				super.setConsumersPerQueue(this.consumerCount);
			}
		}
	}

	@Override
	protected void consumerRemoved(SimpleConsumer consumer) {
		this.inUseConsumerChannels.remove(consumer.getChannel());
		this.whenUsed.remove(consumer);
	}

	/**
	 * Get the channel associated with a direct reply-to consumer.
	 * @return the channel.
	 */
	public Channel getChannel() {
		synchronized (this.consumersMonitor) {
			Channel channel = null;
			while (channel == null) {
				if (!isRunning()) {
					throw new IllegalStateException("Direct reply-to container is not running");
				}
				for (SimpleConsumer consumer : this.consumers) {
					Channel candidate = consumer.getChannel();
					if (candidate.isOpen() && this.inUseConsumerChannels.putIfAbsent(candidate, consumer) == null) {
						channel = candidate;
						this.whenUsed.put(consumer, System.currentTimeMillis());
						break;
					}
				}
				if (channel == null) {
					this.consumerCount++;
					super.setConsumersPerQueue(this.consumerCount);
				}
			}
			return channel;
		}
	}

	/**
	 * Release the consumer associated with the channel for reuse.
	 * Set cancelConsumer to true if the client is not prepared to handle/discard a
	 * late arriving reply.
	 * @param channel the channel.
	 * @param cancelConsumer true to cancel the consumer.
	 * @param message a message to be included in the cancel event if cancelConsumer is true.
	 */
	public void releaseConsumerFor(Channel channel, boolean cancelConsumer, String message) {
		SimpleConsumer consumer = this.inUseConsumerChannels.remove(channel);
		if (consumer != null && cancelConsumer) {
			Assert.isTrue(message != null, "A 'message' is required when 'cancelConsumer' is 'true'");
			consumer.cancelConsumer("Consumer " + this + " canceled due to " + message);
		}
	}

}
