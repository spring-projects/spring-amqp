/*
 * Copyright 2016-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.lang.Nullable;
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

	private final AtomicInteger consumerCount = new AtomicInteger();

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
	public final void addQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final boolean removeQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMessageListener(MessageListener messageListener) {
		if (messageListener instanceof ChannelAwareMessageListener channelAwareMessageListener) {
			super.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
				try {
					channelAwareMessageListener.onMessage(message, channel);
				}
				finally {
					this.inUseConsumerChannels.remove(channel);
				}
			});
		}
		else {
			super.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
				try {
					messageListener.onMessage(message);
				}
				finally {
					this.inUseConsumerChannels.remove(channel);
				}
			});
		}
	}

	@Override
	protected void doStart() {
		if (!isRunning()) {
			this.consumerCount.set(0);
			super.setConsumersPerQueue(0);
			super.doStart();
		}
	}

	@Override
	protected void processMonitorTask() {
		long now = System.currentTimeMillis();
		long reduce;
		this.consumersLock.lock();
		try {
			reduce = this.consumers.stream()
					.filter(c -> this.whenUsed.containsKey(c) && !this.inUseConsumerChannels.containsValue(c)
							&& this.whenUsed.get(c) < now - getIdleEventInterval())
					.count();
		}
		finally {
			this.consumersLock.unlock();
		}
		if (reduce > 0) {
			if (logger.isDebugEnabled()) {
				logger.debug("Reducing idle consumes by " + reduce);
			}
			super.setConsumersPerQueue(
					this.consumerCount.updateAndGet((current) -> (int) Math.max(0, current - reduce)));
		}
	}

	@Override
	protected int findIdleConsumer() {
		for (int i = 0; i < this.consumers.size(); i++) {
			if (!this.inUseConsumerChannels.containsValue(this.consumers.get(i))) {
				return i;
			}
		}
		return -1;
	}

	@Override
	protected void consumerRemoved(SimpleConsumer consumer) {
		this.inUseConsumerChannels.remove(consumer.getChannel());
		this.whenUsed.remove(consumer);
	}

	/**
	 * Get the channel holder associated with a direct reply-to consumer; contains a
	 * consumer epoch to prevent inappropriate releases.
	 * @return the channel holder.
	 */
	public ChannelHolder getChannelHolder() {
		ChannelHolder channelHolder = null;
		while (channelHolder == null) {
			if (!isRunning()) {
				throw new IllegalStateException("Direct reply-to container is not running");
			}
			this.consumersLock.lock();
			try {
				for (SimpleConsumer consumer : this.consumers) {
					Channel candidate = consumer.getChannel();
					if (candidate.isOpen() && this.inUseConsumerChannels.putIfAbsent(candidate, consumer) == null) {
						channelHolder = new ChannelHolder(candidate, consumer.incrementAndGetEpoch());
						this.whenUsed.put(consumer, System.currentTimeMillis());

						break;
					}
				}
			}
			finally {
				this.consumersLock.unlock();
			}
			if (channelHolder == null) {
				try {
					super.setConsumersPerQueue(this.consumerCount.incrementAndGet());
				}
				catch (AmqpTimeoutException timeoutException) {
					// Possibly No available channels in the cache, so come back to consumers
					// iteration until existing is available
					this.consumerCount.decrementAndGet();
				}
			}
		}
		return channelHolder;

	}

	/**
	 * Release the consumer associated with the channel for reuse.
	 * Set cancelConsumer to true if the client is not prepared to handle/discard a
	 * late arriving reply.
	 * @param channelHolder the channel holder.
	 * @param cancelConsumer true to cancel the consumer.
	 * @param message a message to be included in the cancel event if cancelConsumer is true.
	 */
	public void releaseConsumerFor(ChannelHolder channelHolder, boolean cancelConsumer, @Nullable String message) {
		this.consumersLock.lock();
		try {
			SimpleConsumer consumer = this.inUseConsumerChannels.get(channelHolder.getChannel());
			if (consumer != null && consumer.getEpoch() == channelHolder.getConsumerEpoch()) {
				this.inUseConsumerChannels.remove(channelHolder.getChannel());
				if (cancelConsumer) {
					Assert.isTrue(message != null, "A 'message' is required when 'cancelConsumer' is 'true'");
					consumer.cancelConsumer("Consumer " + this + " canceled due to " + message);
				}
			}
		}
		finally {
			this.consumersLock.unlock();
		}
	}

	/**
	 * Holder for a channel; contains a consumer epoch used to prevent inappropriate release
	 * of the consumer after it has been allocated for reuse.
	 */
	public static class ChannelHolder {

		private final Channel channel;

		private final int consumerEpoch;

		ChannelHolder(Channel channel, int consumerEpoch) {
			this.channel = channel;
			this.consumerEpoch = consumerEpoch;
		}

		public Channel getChannel() {
			return this.channel;
		}

		public int getConsumerEpoch() {
			return this.consumerEpoch;
		}

		@Override
		public String toString() {
			return "ChannelHolder [channel=" + this.channel + ", consumerEpoch=" + this.consumerEpoch + "]";
		}

	}

}
