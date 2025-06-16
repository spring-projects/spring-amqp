/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AmqpMessageReturnedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueBuilder.Overflow;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.core.AmqpNackReceivedException;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @since 2.0.5
 *
 */
@RabbitAvailable(queues = RepublishMessageRecovererWithConfirmsIntegrationTests.QUEUE)
class RepublishMessageRecovererWithConfirmsIntegrationTests {

	static final String QUEUE = "RepublishMessageRecovererWithConfirmsIntegrationTests";

	@Test
	void testSimple() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.SIMPLE);
		RabbitTemplate template = new RabbitTemplate(ccf);
		RepublishMessageRecovererWithConfirms recoverer = new RepublishMessageRecovererWithConfirms(template, "",
				QUEUE, ConfirmType.SIMPLE);
		recoverer.recover(MessageBuilder.withBody("foo".getBytes()).build(), new RuntimeException());
		Message received = template.receive(QUEUE, 10_000);
		assertThat(received.getBody()).isEqualTo("foo".getBytes());
		ccf.destroy();
	}

	@Test
	void testCorrelated() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		RabbitTemplate template = new RabbitTemplate(ccf);
		RepublishMessageRecovererWithConfirms recoverer = new RepublishMessageRecovererWithConfirms(template, "",
				QUEUE, ConfirmType.CORRELATED);
		recoverer.recover(MessageBuilder.withBody("foo".getBytes()).build(), new RuntimeException());
		Message received = template.receive(QUEUE, 10_000);
		assertThat(received.getBody()).isEqualTo("foo".getBytes());
		ccf.destroy();
	}

	@Test
	void testCorrelatedNotRoutable() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.setPublisherReturns(true);
		RabbitTemplate template = new RabbitTemplate(ccf);
		template.setMandatory(true);
		RepublishMessageRecovererWithConfirms recoverer = new RepublishMessageRecovererWithConfirms(template, "",
				"bad.route", ConfirmType.CORRELATED);
		try {
			recoverer.recover(MessageBuilder.withBody("foo".getBytes()).build(), new RuntimeException());
			fail("Expected exception");
		}
		catch (AmqpMessageReturnedException ex) {
			assertThat(ex.getReturnedMessage().getBody()).isEqualTo("foo".getBytes());
			assertThat(ex.getReplyText()).isEqualTo("NO_ROUTE");
		}
		ccf.destroy();
	}

	@Test
	void testCorrelatedWithNack() {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		RabbitTemplate template = new RabbitTemplate(ccf);
		RabbitAdmin admin = new RabbitAdmin(ccf);
		Queue queue = QueueBuilder.durable(QUEUE + ".nack")
				.maxLength(1L)
				.overflow(Overflow.rejectPublish)
				.build();
		admin.deleteQueue(queue.getName());
		admin.declareQueue(queue);

		RepublishMessageRecovererWithConfirms recoverer = new RepublishMessageRecovererWithConfirms(template,
				new LiteralExpression(""),
				new SpelExpressionParser().parseExpression("messageProperties.headers[queueName]"),
				ConfirmType.CORRELATED);

		Message message = MessageBuilder.withBody("foo".getBytes()).setHeader("queueName", queue.getName()).build();

		recoverer.recover(message, new RuntimeException());

		assertThatExceptionOfType(AmqpNackReceivedException.class)
				.isThrownBy(() -> recoverer.recover(message, new RuntimeException()));

		admin.deleteQueue(queue.getName());
		ccf.destroy();
	}

}
