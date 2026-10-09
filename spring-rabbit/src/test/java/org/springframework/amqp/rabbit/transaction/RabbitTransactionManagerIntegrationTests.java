/*
 * Copyright 2011-present the original author or authors.
 */

package org.springframework.amqp.rabbit.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author David Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.0
 *
 */
@RabbitAvailable(queues = RabbitTransactionManagerIntegrationTests.ROUTE)
public class RabbitTransactionManagerIntegrationTests {

	public static final String ROUTE = "test.queue.RabbitTransactionManagerIntegrationTests";

	private RabbitTemplate template;

	private TransactionTemplate transactionTemplate;

	@BeforeEach
	public void init() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		RabbitTransactionManager transactionManager = new RabbitTransactionManager(connectionFactory);
		transactionManager.afterPropertiesSet();
		transactionTemplate = new TransactionTemplate(transactionManager);
	}

	@AfterEach
	public void cleanup() throws Exception {
		this.template.stop();
		((DisposableBean) this.template.getConnectionFactory()).destroy();
	}

	@Test
	public void testSendAndReceiveInTransaction() {
		String result = transactionTemplate.execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return (String) template.receiveAndConvert(ROUTE);
		});
		assertThat(result).isEqualTo(null);
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
	}

	@Test
	public void testReceiveInTransaction() {
		template.convertAndSend(ROUTE, "message");
		String result = transactionTemplate.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testReceiveInTransactionWithRollback() {
		// Makes receive (and send in principle) transactional
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		assertThatExceptionOfType(PlannedException.class)
				.isThrownBy(() -> transactionTemplate.execute(status -> {
					template.receiveAndConvert(ROUTE);
					throw new PlannedException();
				}));
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testSendInTransaction() {
		template.setChannelTransacted(true);
		transactionTemplate.executeWithoutResult(status -> template.convertAndSend(ROUTE, "message"));
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@Test
	public void testSendInTransactionWithRollback() {
		template.setChannelTransacted(true);
		assertThatExceptionOfType(PlannedException.class)
				.isThrownBy(() -> transactionTemplate.execute(status -> {
					template.convertAndSend(ROUTE, "message");
					throw new PlannedException();
				}));
		String result = (String) template.receiveAndConvert(ROUTE);
		assertThat(result).isEqualTo(null);
	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {

		PlannedException() {
			super("Planned");
		}

	}

}
