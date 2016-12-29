/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;

import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LongRunningIntegrationTest;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.rabbit.test.RepeatProcessor;
import org.springframework.test.annotation.Repeat;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
public class RabbitTemplatePerformanceIntegrationTests {

	private static final String ROUTE = "test.queue";

	private final RabbitTemplate template = new RabbitTemplate();

	@Rule
	public LongRunningIntegrationTest longTests = new LongRunningIntegrationTest();

	@Rule
	public RepeatProcessor repeat = new RepeatProcessor(4);

	@Rule
	// After the repeat processor, so it only runs once
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.ERROR, RabbitTemplate.class);

	@Rule
	// After the repeat processor, so it only runs once
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(ROUTE);

	private CachingConnectionFactory connectionFactory;

	@Before
	public void declareQueue() {
		if (repeat.isInitialized()) {
			// Important to prevent concurrent re-initialization
			return;
		}
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setChannelCacheSize(repeat.getConcurrency());
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
	}

	@After
	public void cleanUp() {
		if (!repeat.isFinalizing()) {
			return;
		}
		this.template.stop();
		this.connectionFactory.destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	@Repeat(200)
	public void testSendAndReceive() throws Exception {
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		int count = 5;
		while (result == null && count-- > 0) {
			/*
			 * Retry for the purpose of non-transacted case because channel operations are async in that case
			 */
			Thread.sleep(10L);
			result = (String) template.receiveAndConvert(ROUTE);
		}
		assertEquals("message", result);
	}

	@Test
	@Repeat(200)
	public void testSendAndReceiveTransacted() throws Exception {
		template.setChannelTransacted(true);
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
	}

	@Test
	@Repeat(200)
	public void testSendAndReceiveExternalTransacted() throws Exception {
		template.setChannelTransacted(true);
		new TransactionTemplate(new TestTransactionManager()).execute(status -> {
			template.convertAndSend(ROUTE, "message");
			return null;
		});
		template.convertAndSend(ROUTE, "message");
		String result = (String) template.receiveAndConvert(ROUTE);
		assertEquals("message", result);
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		TestTransactionManager() {
			super();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}
