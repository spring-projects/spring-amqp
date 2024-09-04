/*
 * Copyright 2021-2023 the original author or authors.
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

package org.springframework.rabbit.stream.config;

import java.lang.reflect.Method;

import org.aopalliance.aop.Advice;

import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.config.BaseRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.listener.ConsumerCustomizer;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import org.springframework.rabbit.stream.listener.adapter.StreamMessageListenerAdapter;
import org.springframework.rabbit.stream.micrometer.RabbitStreamListenerObservationConvention;
import org.springframework.util.Assert;

import com.rabbitmq.stream.Environment;

/**
 * Factory for StreamListenerContainer.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamRabbitListenerContainerFactory
		extends BaseRabbitListenerContainerFactory<StreamListenerContainer> {

	private final Environment environment;

	private boolean nativeListener;

	private ConsumerCustomizer consumerCustomizer;

	private ContainerCustomizer<StreamListenerContainer> containerCustomizer;

	private RabbitStreamListenerObservationConvention streamListenerObservationConvention;

	/**
	 * Construct an instance using the provided environment.
	 * @param environment the environment.
	 */
	public StreamRabbitListenerContainerFactory(Environment environment) {
		Assert.notNull(environment, "'environment' cannot be null");
		this.environment = environment;
	}

	/**
	 * Set to true to create a container supporting a native RabbitMQ Stream message.
	 * @param nativeListener true for native listeners.
	 */
	public void setNativeListener(boolean nativeListener) {
		this.nativeListener = nativeListener;
	}

	/**
	 * Customize the consumer builder before it is built.
	 * @param consumerCustomizer the customizer.
	 */
	public void setConsumerCustomizer(ConsumerCustomizer consumerCustomizer) {
		this.consumerCustomizer = consumerCustomizer;
	}

	/**
	 * Set a {@link ContainerCustomizer} that is invoked after a container is created and
	 * configured to enable further customization of the container.
	 * @param containerCustomizer the customizer.
	 */
	public void setContainerCustomizer(ContainerCustomizer<StreamListenerContainer> containerCustomizer) {
		this.containerCustomizer = containerCustomizer;
	}

	/**
	 * Set a {@link RabbitStreamListenerObservationConvention} that is used when receiving
	 * native stream messages.
	 * @param streamListenerObservationConvention the convention.
	 * @since 3.0.5
	 */
	public void setStreamListenerObservationConvention(
			RabbitStreamListenerObservationConvention streamListenerObservationConvention) {

		this.streamListenerObservationConvention = streamListenerObservationConvention;
	}

	@Override
	public StreamListenerContainer createListenerContainer(RabbitListenerEndpoint endpoint) {
		if (endpoint instanceof MethodRabbitListenerEndpoint methodRabbitListenerEndpoint && this.nativeListener) {
			methodRabbitListenerEndpoint.setAdapterProvider(
					(boolean batch, Object bean, Method method, boolean returnExceptions,
							RabbitListenerErrorHandler errorHandler, @Nullable BatchingStrategy batchingStrategy) -> {

								Assert.isTrue(!batch, "Batch listeners are not supported by the stream container");
								return new StreamMessageListenerAdapter(bean, method, returnExceptions, errorHandler);
							});
		}
		StreamListenerContainer container = createContainerInstance();
		Advice[] adviceChain = getAdviceChain();
		JavaUtils.INSTANCE
				.acceptIfNotNull(getApplicationContext(), container::setApplicationContext)
				.acceptIfNotNull(this.consumerCustomizer, container::setConsumerCustomizer)
				.acceptIfNotNull(adviceChain, container::setAdviceChain)
				.acceptIfNotNull(getMicrometerEnabled(), container::setMicrometerEnabled)
				.acceptIfNotNull(getObservationEnabled(), container::setObservationEnabled)
				.acceptIfNotNull(this.streamListenerObservationConvention, container::setObservationConvention);
		applyCommonOverrides(endpoint, container);
		if (this.containerCustomizer != null) {
			this.containerCustomizer.configure(container);
		}
		return container;
	}

	/**
	 * Create an instance of the listener container.
	 * @return the container.
	 */
	protected StreamListenerContainer createContainerInstance() {
		return new StreamListenerContainer(this.environment);
	}

}
