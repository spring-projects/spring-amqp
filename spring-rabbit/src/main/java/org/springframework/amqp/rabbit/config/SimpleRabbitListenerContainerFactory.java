/*
 * Copyright 2014-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

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
 * @author Jeonggi Kim
 *
 * @since 1.4
 */
public class SimpleRabbitListenerContainerFactory
		extends AbstractRabbitListenerContainerFactory<SimpleMessageListenerContainer> {

	private @Nullable Integer batchSize;

	private @Nullable Integer concurrentConsumers;

	private @Nullable Integer maxConcurrentConsumers;

	private @Nullable Long startConsumerMinInterval;

	private @Nullable Long stopConsumerMinInterval;

	private @Nullable Integer consecutiveActiveTrigger;

	private @Nullable Integer consecutiveIdleTrigger;

	private @Nullable Long receiveTimeout;

	private @Nullable Long batchReceiveTimeout;

	private @Nullable Boolean consumerBatchEnabled;

	private @Nullable Boolean enforceImmediateAckForManual;

	/**
	 * @param batchSize the batch size.
	 * @since 2.2
	 * @see SimpleMessageListenerContainer#setBatchSize
	 */
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
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
	 * The number of milliseconds of timeout for gathering batch messages.
	 * It limits the time to wait to fill batchSize.
	 * Default is 0 (no timeout).
	 * @param batchReceiveTimeout the timeout for gathering batch messages.
	 * @since 3.1.2
	 * @see SimpleMessageListenerContainer#setBatchReceiveTimeout
	 * @see #setBatchSize(Integer)
	 */
	public void setBatchReceiveTimeout(Long batchReceiveTimeout) {
		this.batchReceiveTimeout = batchReceiveTimeout;
	}

	/**
	 * Set to true to present a list of messages based on the {@link #setBatchSize(Integer)},
	 * if the listener supports it. Starting with version 3.0, setting this to true will
	 * also {@link #setBatchListener(boolean)} to true.
	 * @param consumerBatchEnabled true to create message batches in the container.
	 * @since 2.2
	 * @see #setBatchSize(Integer)
	 * @see #setBatchListener(boolean)
	 */
	public void setConsumerBatchEnabled(boolean consumerBatchEnabled) {
		this.consumerBatchEnabled = consumerBatchEnabled;
		if (consumerBatchEnabled) {
			setBatchListener(true);
		}
	}

	/**
	 * Set to {@code true} to enforce {@link com.rabbitmq.client.Channel#basicAck(long, boolean)}
	 * for {@link org.springframework.amqp.core.AcknowledgeMode#MANUAL}
	 * when {@link org.springframework.amqp.ImmediateAcknowledgeAmqpException} is thrown.
	 * This might be a tentative solution to not break behavior for current minor version.
	 * @param enforceImmediateAckForManual the flag to ack message for MANUAL mode on ImmediateAcknowledgeAmqpException
	 * @since 3.1.2
	 */
	public void setEnforceImmediateAckForManual(Boolean enforceImmediateAckForManual) {
		this.enforceImmediateAckForManual = enforceImmediateAckForManual;
	}

	@Override
	protected SimpleMessageListenerContainer createContainerInstance() {
		return new SimpleMessageListenerContainer();
	}

	@Override
	protected void initializeContainer(SimpleMessageListenerContainer instance,
			@Nullable RabbitListenerEndpoint endpoint) {

		super.initializeContainer(instance, endpoint);

		JavaUtils javaUtils = JavaUtils.INSTANCE
				.acceptIfNotNull(this.batchSize, instance::setBatchSize);
		String concurrency = null;
		if (endpoint != null) {
			concurrency = endpoint.getConcurrency();
			javaUtils.acceptIfNotNull(concurrency, instance::setConcurrency);
		}
		javaUtils
				.acceptIfCondition(concurrency == null && this.concurrentConsumers != null, this.concurrentConsumers,
						instance::setConcurrentConsumers)
				.acceptIfCondition((concurrency == null || !(concurrency.contains("-")))
								&& this.maxConcurrentConsumers != null,
						this.maxConcurrentConsumers, instance::setMaxConcurrentConsumers)
				.acceptIfNotNull(this.startConsumerMinInterval, instance::setStartConsumerMinInterval)
				.acceptIfNotNull(this.stopConsumerMinInterval, instance::setStopConsumerMinInterval)
				.acceptIfNotNull(this.consecutiveActiveTrigger, instance::setConsecutiveActiveTrigger)
				.acceptIfNotNull(this.consecutiveIdleTrigger, instance::setConsecutiveIdleTrigger)
				.acceptIfNotNull(this.receiveTimeout, instance::setReceiveTimeout)
				.acceptIfNotNull(this.batchReceiveTimeout, instance::setBatchReceiveTimeout)
				.acceptIfNotNull(this.enforceImmediateAckForManual, instance::setEnforceImmediateAckForManual);
		if (Boolean.TRUE.equals(this.consumerBatchEnabled)) {
			instance.setConsumerBatchEnabled(true);
			/*
			 * 'batchListener=true' turns off container debatching by default, it must be
			 * true when consumer batching is enabled.
			 */
			instance.setDeBatchingEnabled(true);
		}
	}

}
