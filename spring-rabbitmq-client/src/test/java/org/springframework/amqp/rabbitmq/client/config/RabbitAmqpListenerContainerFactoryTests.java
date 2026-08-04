/*
 * Copyright 2026-present the original author or authors.
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

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.listener.RabbitAmqpListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Martin Ferret
 *
 * @since 4.2
 */
public class RabbitAmqpListenerContainerFactoryTests {

	private final AmqpConnectionFactory connectionFactory = mock();

	private final RabbitAmqpListenerContainerFactory factory =
			new RabbitAmqpListenerContainerFactory(this.connectionFactory);

	@Test
	void endpointBatchOverridesWinOverFactory() {
		this.factory.setBatchSize(10);
		this.factory.setBatchReceiveTimeout(1500L);

		MethodRabbitListenerEndpoint endpoint = createEndpoint();
		endpoint.setBatchSize(5);
		endpoint.setBatchReceiveTimeout(2500L);

		RabbitAmqpListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertThat(TestUtils.<Integer>getPropertyValue(container, "batchSize")).isEqualTo(5);
		assertThat(TestUtils.<Duration>getPropertyValue(container, "batchReceiveDuration"))
				.isEqualTo(Duration.ofMillis(2500));
	}

	@Test
	void factoryBatchSettingsUsedWhenEndpointDoesNotOverride() {
		this.factory.setBatchSize(10);
		this.factory.setBatchReceiveTimeout(1500L);

		RabbitAmqpListenerContainer container = this.factory.createListenerContainer(createEndpoint());

		assertThat(TestUtils.<Integer>getPropertyValue(container, "batchSize")).isEqualTo(10);
		assertThat(TestUtils.<Duration>getPropertyValue(container, "batchReceiveDuration"))
				.isEqualTo(Duration.ofMillis(1500));
	}

	@Test
	void endpointBatchSizeTurnsOnBatchListener() {
		MethodRabbitListenerEndpoint endpoint = createEndpoint();
		endpoint.setBatchSize(5);

		RabbitAmqpListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertThat(endpoint.getBatchListener()).isTrue();
		assertThat(TestUtils.<Integer>getPropertyValue(container, "batchSize")).isEqualTo(5);
	}

	private static MethodRabbitListenerEndpoint createEndpoint() {
		DefaultMessageHandlerMethodFactory methodFactory = new DefaultMessageHandlerMethodFactory();
		methodFactory.setBeanFactory(new StaticListableBeanFactory());
		methodFactory.afterPropertiesSet();

		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setBean(new SampleBean());
		endpoint.setMethod(ReflectionUtils.findMethod(SampleBean.class, "listen", List.class));
		endpoint.setMessageHandlerMethodFactory(methodFactory);
		endpoint.setQueueNames("myQueue");
		return endpoint;
	}

	static class SampleBean {

		void listen(List<String> messages) {
		}

	}

}
