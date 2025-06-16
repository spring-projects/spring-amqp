/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.scheduling.TaskScheduler;

/**
 * A {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
 * implementation to build a regular {@link DirectMessageListenerContainer}.
 *
 * @author Gary Russell
 * @author Sud Ramasamy
 * @since 2.0
 */
public class DirectRabbitListenerContainerFactory
		extends AbstractRabbitListenerContainerFactory<DirectMessageListenerContainer> {

	private TaskScheduler taskScheduler;

	private Long monitorInterval;

	private Integer consumersPerQueue = 1;

	private Integer messagesPerAck;

	private Long ackTimeout;

	/**
	 * Set the task scheduler to use for the task that monitors idle containers and
	 * failed consumers.
	 * @param taskScheduler the scheduler.
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
	}

	/**
	 * Set how often to run a task to check for failed consumers and idle containers.
	 * @param monitorInterval the interval; default 10000 but it will be adjusted down
	 * to the smallest of this, {@link #setIdleEventInterval(Long) idleEventInterval} / 2
	 * (if configured) or
	 * {@link #setFailedDeclarationRetryInterval(Long) failedDeclarationRetryInterval}.
	 */
	public void setMonitorInterval(long monitorInterval) {
		this.monitorInterval = monitorInterval;
	}

	/**
	 * Each queue runs in its own consumer; set this property to create multiple
	 * consumers for each queue.
	 * If the container is already running, the number of consumers per queue will
	 * be adjusted up or down as necessary.
	 * @param consumersPerQueue the consumers per queue.
	 */
	public void setConsumersPerQueue(Integer consumersPerQueue) {
		this.consumersPerQueue = consumersPerQueue;
	}

	/**
	 * Set the number of messages to receive before acknowledging (success).
	 * A failed message will short-circuit this counter.
	 * @param messagesPerAck the number of messages.
	 * @see #setAckTimeout(Long)
	 */
	public void setMessagesPerAck(Integer messagesPerAck) {
		this.messagesPerAck = messagesPerAck;
	}

	/**
	 * An approximate timeout; when {@link #setMessagesPerAck(Integer) messagesPerAck} is
	 * greater than 1, and this time elapses since the last ack, the pending acks will be
	 * sent either when the next message arrives, or a short time later if no additional
	 * messages arrive. In that case, the actual time depends on the
	 * {@link #setMonitorInterval(long) monitorInterval}.
	 * @param ackTimeout the timeout in milliseconds (default 20000);
	 * @see #setMessagesPerAck(Integer)
	 */
	public void setAckTimeout(Long ackTimeout) {
		this.ackTimeout = ackTimeout;
	}

	@Override
	protected DirectMessageListenerContainer createContainerInstance() {
		return new DirectMessageListenerContainer();
	}

	@Override
	protected void initializeContainer(DirectMessageListenerContainer instance, RabbitListenerEndpoint endpoint) {
		super.initializeContainer(instance, endpoint);

		JavaUtils javaUtils = JavaUtils.INSTANCE.acceptIfNotNull(this.taskScheduler, instance::setTaskScheduler)
			.acceptIfNotNull(this.monitorInterval, instance::setMonitorInterval)
			.acceptIfNotNull(this.messagesPerAck, instance::setMessagesPerAck)
			.acceptIfNotNull(this.ackTimeout, instance::setAckTimeout);
		if (endpoint != null && endpoint.getConcurrency() != null) {
			try {
				instance.setConsumersPerQueue(Integer.parseInt(endpoint.getConcurrency()));
			}
			catch (NumberFormatException e) {
				throw new IllegalStateException("Failed to parse concurrency: " + e.getMessage(), e);
			}
		}
		else {
			javaUtils.acceptIfNotNull(this.consumersPerQueue, instance::setConsumersPerQueue);
		}
	}

}
