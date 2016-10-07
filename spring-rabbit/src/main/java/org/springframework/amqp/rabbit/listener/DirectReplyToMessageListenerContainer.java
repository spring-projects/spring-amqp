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

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import com.rabbitmq.client.Channel;

/**
 * Listener container for Direct ReplyTo only listens to the pseudo queue
 * {@link Address#AMQ_RABBITMQ_REPLY_TO} with a single consumer.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class DirectReplyToMessageListenerContainer extends DirectMessageListenerContainer {

	public DirectReplyToMessageListenerContainer(ConnectionFactory connectionFactory) {
		super(connectionFactory);
		setQueueNames(Address.AMQ_RABBITMQ_REPLY_TO);
		setAcknowledgeMode(AcknowledgeMode.NONE);
	}

	@Override
	public void setConsumersPerQueue(int consumersPerQueue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMonitorInterval(long monitorInterval) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeQueueNames(String... queueNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeQueues(Queue... queues) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Get the channel associated with the single direct reply-to consumer.
	 * If the consumer has exited, the container will be stopped.
	 * @return the channel or null if there is no consumer.
	 */
	public Channel getChannel() {
		synchronized (this.consumersMonitor) {
			if (this.consumers.size() == 1) {
				return this.consumers.get(0).getChannel();
			}
			else {
				stop();
				return null;
			}
		}
	}

}
