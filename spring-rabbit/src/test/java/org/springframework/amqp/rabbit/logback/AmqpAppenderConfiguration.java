/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.logback;

import javax.annotation.PreDestroy;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * @author Jon Brisbin
 */
@Configuration
public class AmqpAppenderConfiguration {

	private static final String QUEUE = "amqp.appender.test";

	private static final String EXCHANGE = "logs";

	private static final String ROUTING_KEY = "AmqpAppenderTest.#";

	private static final String ENCODED_ROUTING_KEY = "encoded-AmqpAppenderTest.#";

	@Bean
	public SingleConnectionFactory connectionFactory() {
		return new SingleConnectionFactory("localhost");
	}

	@PreDestroy
	public void destroy() {
		SingleConnectionFactory cf = new SingleConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		admin.deleteExchange(EXCHANGE);
		admin.deleteQueue(QUEUE);
		admin.deleteQueue("encoded-" + QUEUE);
		cf.destroy();
	}

	@Bean
	public TopicExchange testExchange() {
		return new TopicExchange(EXCHANGE, true, false);
	}

	@Bean
	public Queue testQueue() {
		return new Queue(QUEUE);
	}

	@Bean
	public Queue encodedQueue() {
		return new Queue("encoded-" + QUEUE);
	}

	@Bean
	public Binding testBinding() {
		return BindingBuilder.bind(testQueue()).to(testExchange()).with(ROUTING_KEY);
	}

	@Bean
	public Binding encodedBinding() {
		return BindingBuilder.bind(encodedQueue()).to(testExchange()).with(ENCODED_ROUTING_KEY);
	}

	@Bean
	public RabbitAdmin rabbitAdmin() {
		return new RabbitAdmin(connectionFactory());
	}

	@Bean
	public RabbitTemplate rabbitTemplate() {
		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());
		rabbitTemplate.setReceiveTimeout(10_000);
		return rabbitTemplate;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public SimpleMessageListenerContainer listenerContainer() {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
		container.setQueues(testQueue());
		// container.setMessageListener(testListener(4));
		container.setAutoStartup(false);
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);

		return container;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public TestListener testListener(int count) {
		return new TestListener(count);
	}
}
