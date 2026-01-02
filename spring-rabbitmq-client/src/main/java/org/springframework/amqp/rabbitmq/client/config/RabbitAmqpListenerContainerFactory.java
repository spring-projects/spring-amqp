/*
 * Copyright 2021-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client.config;

import java.util.Arrays;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.config.BaseRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpListenerContainer;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpMessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.scheduling.TaskScheduler;

/**
 * Factory for {@link RabbitAmqpListenerContainer}.
 * To use it as default one, has to be configured with a
 * {@link org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor#DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY_BEAN_NAME}.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 *
 */
public class RabbitAmqpListenerContainerFactory
		extends BaseRabbitListenerContainerFactory<RabbitAmqpListenerContainer> {

	private final AmqpConnectionFactory connectionFactory;

	private @Nullable ContainerCustomizer<RabbitAmqpListenerContainer> containerCustomizer;

	private MessagePostProcessor @Nullable [] afterReceivePostProcessors;

	private @Nullable Integer batchSize;

	private @Nullable Long batchReceiveTimeout;

	private @Nullable TaskScheduler taskScheduler;

	private @Nullable MessageConverter messageConverter;

	/**
	 * Construct an instance using the provided {@link AmqpConnectionFactory}.
	 * @param connectionFactory the connection.
	 */
	public RabbitAmqpListenerContainerFactory(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param containerCustomizer the customizer.
	 */
	public void setContainerCustomizer(ContainerCustomizer<RabbitAmqpListenerContainer> containerCustomizer) {
		this.containerCustomizer = containerCustomizer;
	}

	/**
	 * Set {@link MessagePostProcessor}s that will be applied after message reception, before
	 * invoking the {@link MessageListener}. Often used to decompress data.  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post-processor.
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		this.afterReceivePostProcessors = Arrays.copyOf(afterReceivePostProcessors, afterReceivePostProcessors.length);
	}

	/**
	 * The size of the batch of messages to process.
	 * This is only option (if {@code batchSize > 1}) which turns the target listener container into a batch mode.
	 * @param batchSize the batch size.
	 * @see RabbitAmqpListenerContainer#setBatchSize
	 * @see #setBatchReceiveTimeout(Long)
	 */
	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * The number of milliseconds of timeout for gathering batch messages.
	 * It limits the time to wait to fill batchSize.
	 * Default is 30 seconds.
	 * @param batchReceiveTimeout the timeout for gathering batch messages.
	 * @see RabbitAmqpListenerContainer#setBatchReceiveTimeout
	 * @see #setBatchSize(Integer)
	 */
	public void setBatchReceiveTimeout(Long batchReceiveTimeout) {
		this.batchReceiveTimeout = batchReceiveTimeout;
	}

	/**
	 * Configure a {@link TaskScheduler} to release not fulfilled batches after timeout.
	 * @param taskScheduler the {@link TaskScheduler} to use.
	 * @see RabbitAmqpListenerContainer#setTaskScheduler(TaskScheduler)
	 * @see #setBatchReceiveTimeout(Long)
	 */
	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
	}

	/**
	 * Set a {@link MessageConverter} for all the listeners based on this factory.
	 * @param messageConverter the message converter to use
	 * @since 4.0.2
	 * @see RabbitListenerEndpoint#setMessageConverter(MessageConverter)
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public RabbitAmqpListenerContainer createListenerContainer(@Nullable RabbitListenerEndpoint endpoint) {
		if (endpoint instanceof MethodRabbitListenerEndpoint methodRabbitListenerEndpoint) {
			JavaUtils.INSTANCE
					.acceptIfCondition(this.batchSize != null && this.batchSize > 1,
							true,
							methodRabbitListenerEndpoint::setBatchListener);

			methodRabbitListenerEndpoint.setAdapterProvider(
					(batch, bean, method, returnExceptions, errorHandler, batchingStrategy) ->
							new RabbitAmqpMessageListenerAdapter(bean, method, returnExceptions, errorHandler, batch));
		}
		if (this.messageConverter != null && endpoint != null && endpoint.getMessageConverter() == null) {
			endpoint.setMessageConverter(this.messageConverter);
		}
		RabbitAmqpListenerContainer container = createContainerInstance();
		JavaUtils.INSTANCE
				.acceptIfNotNull(getAdviceChain(), container::setAdviceChain)
				.acceptIfNotNull(getDefaultRequeueRejected(), container::setDefaultRequeue)
				.acceptIfNotNull(this.afterReceivePostProcessors, container::setAfterReceivePostProcessors)
				.acceptIfNotNull(this.batchSize, container::setBatchSize)
				.acceptIfNotNull(this.batchReceiveTimeout, container::setBatchReceiveTimeout)
				.acceptIfNotNull(this.taskScheduler, container::setTaskScheduler);

		applyCommonOverrides(endpoint, container);

		if (endpoint != null) {
			JavaUtils.INSTANCE
					.acceptIfNotNull(endpoint.getAckMode(),
							(ackMode) -> container.setAutoSettle(!ackMode.isManual()))
					.acceptIfNotNull(endpoint.getConcurrency(),
							(concurrency) -> container.setConsumersPerQueue(Integer.parseInt(concurrency)));
		}
		if (this.containerCustomizer != null) {
			this.containerCustomizer.configure(container);
		}
		return container;
	}

	protected RabbitAmqpListenerContainer createContainerInstance() {
		return new RabbitAmqpListenerContainer(this.connectionFactory);
	}

}
