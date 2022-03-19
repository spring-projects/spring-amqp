/*
 * Copyright 2002-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.connection.PooledChannelConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author Leonardo Ferreira
 * @since 2.4.4
 */
@RabbitAvailable(queues = RabbitTemplateRoutingConnectionFactoryIntegrationTests.ROUTE)
class RabbitTemplateRoutingConnectionFactoryIntegrationTests {

	public static final String ROUTE = "test.queue.RabbitTemplateRoutingConnectionFactoryIntegrationTests";

	private static RabbitTemplate rabbitTemplate;

	@BeforeAll
	static void create() {
		final com.rabbitmq.client.ConnectionFactory cf = new com.rabbitmq.client.ConnectionFactory();
		cf.setHost("localhost");
		cf.setPort(BrokerTestUtils.getPort());

		CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(cf);

		cachingConnectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);

		PooledChannelConnectionFactory pooledChannelConnectionFactory = new PooledChannelConnectionFactory(cf);

		Map<Object, ConnectionFactory> connectionFactoryMap = new HashMap<>(2);
		connectionFactoryMap.put("true", cachingConnectionFactory);
		connectionFactoryMap.put("false", pooledChannelConnectionFactory);

		final AbstractRoutingConnectionFactory routingConnectionFactory = new SimpleRoutingConnectionFactory();
		routingConnectionFactory.setConsistentConfirmsReturns(false);
		routingConnectionFactory.setDefaultTargetConnectionFactory(pooledChannelConnectionFactory);
		routingConnectionFactory.setTargetConnectionFactories(connectionFactoryMap);

		rabbitTemplate = new RabbitTemplate(routingConnectionFactory);

		final Expression sendExpression = new SpelExpressionParser().parseExpression(
				"messageProperties.headers['x-use-publisher-confirms'] ?: false");
		rabbitTemplate.setSendConnectionFactorySelectorExpression(sendExpression);
	}

	@AfterAll
	static void cleanUp() {
		rabbitTemplate.destroy();
	}

	@Test
	void sendWithoutConfirmsTest() {
		final String payload = UUID.randomUUID().toString();
		rabbitTemplate.convertAndSend(ROUTE, (Object) payload, new CorrelationData());
		assertThat(rabbitTemplate.getUnconfirmedCount()).isZero();

		final Message received = rabbitTemplate.receive(ROUTE, Duration.ofSeconds(3).toMillis());
		assertThat(received).isNotNull();
		final String receivedPayload = new String(received.getBody());

		assertThat(receivedPayload).isEqualTo(payload);
	}

	@Test
	void sendWithConfirmsTest() throws Exception {
		final String payload = UUID.randomUUID().toString();
		final Message message = MessageBuilder.withBody(payload.getBytes(StandardCharsets.UTF_8))
				.setHeader("x-use-publisher-confirms", "true").build();

		final CorrelationData correlationData = new CorrelationData();
		rabbitTemplate.send(ROUTE, message, correlationData);
		assertThat(rabbitTemplate.getUnconfirmedCount()).isEqualTo(1);

		final CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);

		assertThat(confirm.isAck()).isTrue();

		final Message received = rabbitTemplate.receive(ROUTE, Duration.ofSeconds(10).toMillis());
		assertThat(received).isNotNull();
		final String receivedPayload = new String(received.getBody());

		assertThat(receivedPayload).isEqualTo(payload);
	}

}
