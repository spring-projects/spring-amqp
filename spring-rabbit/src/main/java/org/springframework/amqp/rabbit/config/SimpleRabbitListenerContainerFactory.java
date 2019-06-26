/*
 * Copyright 2014-2019 the original author or authors.
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

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.utils.JavaUtils;

/**
 * A {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
 * implementation to build a regular {@link SimpleMessageListenerContainer}.
 *
 * <p>
 * This should be the default for most users and a good transition paths for those that
 * are used to build such container definition manually.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dustin Schultz
 *
 * @since 1.4
 */
public class SimpleRabbitListenerContainerFactory
		extends AbstractRabbitListenerContainerFactory<SimpleMessageListenerContainer> {

	private Integer txSize;

	private Integer concurrentConsumers;

	private Integer maxConcurrentConsumers;

	private Long startConsumerMinInterval;

	private Long stopConsumerMinInterval;

	private Integer consecutiveActiveTrigger;

	private Integer consecutiveIdleTrigger;

	private Long receiveTimeout;

	private Boolean deBatchingEnabled;

	/**
	 * @param txSize the transaction size.
	 * @see SimpleMessageListenerContainer#setTxSize
	 */
	public void setTxSize(Integer txSize) {
		this.txSize = txSize;
	}

	/**
	 * @param concurrency the minimum number of consumers to create.
	 * @see SimpleMessageListenerContainer#setConcurrentConsumers
	 */
	public void setConcurrentConsumers(Integer concurrency) {
		this.concurrentConsumers = concurrency;
	}

	/**
	 * @param maxConcurrency the maximum number of consumers.
	 * @see SimpleMessageListenerContainer#setMaxConcurrentConsumers
	 */
	public void setMaxConcurrentConsumers(Integer maxConcurrency) {
		this.maxConcurrentConsumers = maxConcurrency;
	}

	/**
	 * @param minStartInterval The minimum interval between new consumer starts.
	 * @see SimpleMessageListenerContainer#setStartConsumerMinInterval
	 */
	public void setStartConsumerMinInterval(Long minStartInterval) {
		this.startConsumerMinInterval = minStartInterval;
	}

	/**
	 * @param minStopInterval The minimum interval between consumer stops.
	 * @see SimpleMessageListenerContainer#setStopConsumerMinInterval
	 */
	public void setStopConsumerMinInterval(Long minStopInterval) {
		this.stopConsumerMinInterval = minStopInterval;
	}

	/**
	 * @param minConsecutiveActive The number of consecutive receives to trigger a new consumer.
	 * @see SimpleMessageListenerContainer#setConsecutiveActiveTrigger
	 */
	public void setConsecutiveActiveTrigger(Integer minConsecutiveActive) {
		this.consecutiveActiveTrigger = minConsecutiveActive;
	}

	/**
	 * @param minConsecutiveIdle The number of consecutive timeouts to trigger stopping a consumer.
	 * @see SimpleMessageListenerContainer#setConsecutiveIdleTrigger
	 */
	public void setConsecutiveIdleTrigger(Integer minConsecutiveIdle) {
		this.consecutiveIdleTrigger = minConsecutiveIdle;
	}

	/**
	 * @param receiveTimeout the timeout.
	 * @see SimpleMessageListenerContainer#setReceiveTimeout
	 */
	public void setReceiveTimeout(Long receiveTimeout) {
		this.receiveTimeout = receiveTimeout;
	}

	/**
	 * Determine whether or not the container should de-batch batched
	 * messages (true) or call the listener with the batch (false). Default: true.
	 * @param deBatchingEnabled whether or not to disable de-batching of messages.
	 * @see SimpleMessageListenerContainer#setDeBatchingEnabled(boolean)
	 */
	public void setDeBatchingEnabled(final Boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	@Override
	protected SimpleMessageListenerContainer createContainerInstance() {
		return new SimpleMessageListenerContainer();
	}

	@Override
	protected void initializeContainer(SimpleMessageListenerContainer instance, RabbitListenerEndpoint endpoint) {
		super.initializeContainer(instance, endpoint);

		JavaUtils javaUtils = JavaUtils.INSTANCE
			.acceptIfNotNull(this.txSize, instance::setBatchSize);
		String concurrency = null;
		if (endpoint != null) {
			concurrency = endpoint.getConcurrency();
			javaUtils.acceptIfNotNull(concurrency, instance::setConcurrency);
		}
		javaUtils
			.acceptIfCondition(concurrency == null && this.concurrentConsumers != null, this.concurrentConsumers,
				instance::setConcurrentConsumers)
			.acceptIfCondition((concurrency == null || !(concurrency.contains("-"))) && this.maxConcurrentConsumers != null,
				this.maxConcurrentConsumers, instance::setMaxConcurrentConsumers)
			.acceptIfNotNull(this.startConsumerMinInterval, instance::setStartConsumerMinInterval)
			.acceptIfNotNull(this.stopConsumerMinInterval, instance::setStopConsumerMinInterval)
			.acceptIfNotNull(this.consecutiveActiveTrigger, instance::setConsecutiveActiveTrigger)
			.acceptIfNotNull(this.consecutiveIdleTrigger, instance::setConsecutiveIdleTrigger)
			.acceptIfNotNull(this.receiveTimeout, instance::setReceiveTimeout)
			.acceptIfNotNull(this.deBatchingEnabled, instance::setDeBatchingEnabled);
	}

}
