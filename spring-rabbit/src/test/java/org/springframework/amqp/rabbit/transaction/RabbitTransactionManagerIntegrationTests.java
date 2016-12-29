/*
 * Copyright 2011-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author David Syer
 * @author Gunnar Hillert
 * @since 1.0
 *
 */
public class RabbitTransactionManagerIntegrationTests {

	private static final String ROUTE = "test.queue";

	private RabbitTemplate template;

	private TransactionTemplate transactionTemplate;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	@Before
	public void init() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		RabbitTransactionManager transactionManager = new RabbitTransactionManager(connectionFactory);
		transactionTemplate = new TransactionTemplate(transactionManager);
	}

	@After
	public void cleanup() throws Exception {
		this.template.stop();
		((DisposableBean) this.template.getConnectionFactory()).destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testSendAndReceiveInTransaction() throws Exception {
		String result = transactionTemplate.execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return (String) template.receiveAndConvert(ROUTE);
		});
		assertEquals(null, result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
	}

	@Test
	public void testReceiveInTransaction() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = transactionTemplate.execute(status -> (String) template.receiveAndConvert(ROUTE));
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testReceiveInTransactionWithRollback() throws Exception {
		// Makes receive (and send in principle) transactional
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		try {
			transactionTemplate.execute(status -> {
				template.receiveAndConvert(ROUTE);
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInTransaction() throws Exception {
		template.setChannelTransacted(true);
		transactionTemplate.execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return null;
		});
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
		result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@Test
	public void testSendInTransactionWithRollback() throws Exception {
		template.setChannelTransacted(true);
		try {
			transactionTemplate.execute(status -> {
				template.convertAndSend(ROUTE, "message");
				throw new PlannedException();
			});
			fail("Expected PlannedException");
		}
		catch (PlannedException e) {
			// Expected
		}
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals(null, result);
	}

	@SuppressWarnings("serial")
	private class PlannedException extends RuntimeException {
		PlannedException() {
			super("Planned");
		}
	}

}
