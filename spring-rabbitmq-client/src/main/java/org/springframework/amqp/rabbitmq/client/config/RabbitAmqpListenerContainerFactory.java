/*
 * Copyright 2021-2025 the original author or authors.
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

import com.rabbitmq.client.amqp.Connection;
import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.config.BaseRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpListenerContainer;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpMessageListenerAdapter;
import org.springframework.amqp.utils.JavaUtils;

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

	private final Connection connection;

	private @Nullable ContainerCustomizer<RabbitAmqpListenerContainer> containerCustomizer;

	/**
	 * Construct an instance using the provided amqpConnection.
	 * @param amqpConnection the connection.
	 */
	public RabbitAmqpListenerContainerFactory(Connection amqpConnection) {
		this.connection = amqpConnection;
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param containerCustomizer the customizer.
	 */
	public void setContainerCustomizer(ContainerCustomizer<RabbitAmqpListenerContainer> containerCustomizer) {
		this.containerCustomizer = containerCustomizer;
	}

	@Override
	public RabbitAmqpListenerContainer createListenerContainer(@Nullable RabbitListenerEndpoint endpoint) {
		if (endpoint instanceof MethodRabbitListenerEndpoint methodRabbitListenerEndpoint) {
			methodRabbitListenerEndpoint.setAdapterProvider(
					(batch, bean, method, returnExceptions, errorHandler, batchingStrategy) ->
							new RabbitAmqpMessageListenerAdapter(bean, method, returnExceptions, errorHandler));
		}
		RabbitAmqpListenerContainer container = createContainerInstance();
		Advice[] adviceChain = getAdviceChain();
		JavaUtils.INSTANCE
				.acceptIfNotNull(adviceChain, container::setAdviceChain)
				.acceptIfNotNull(getDefaultRequeueRejected(), container::setDefaultRequeue);

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
		return new RabbitAmqpListenerContainer(this.connection);
	}

}
