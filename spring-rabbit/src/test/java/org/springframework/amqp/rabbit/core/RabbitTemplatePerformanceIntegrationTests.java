/*
 * Copyright 2002-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
@RabbitAvailable(queues = RabbitTemplatePerformanceIntegrationTests.ROUTE)
@LogLevels(level = "ERROR", classes = RabbitTemplate.class)
public class RabbitTemplatePerformanceIntegrationTests {

	public static final String ROUTE = "test.queue.RabbitTemplatePerformanceIntegrationTests";

	private static final RabbitTemplate template = new RabbitTemplate();

	private static final ExecutorService exec = Executors.newFixedThreadPool(4);

	private static CachingConnectionFactory connectionFactory;

	@BeforeAll
	public static void declareQueue() {
		connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
	}

	@AfterAll
	public static void cleanUp() {
		template.stop();
		connectionFactory.destroy();
		exec.shutdownNow();
	}

	@RepeatedTest(50)
	public void testSendAndReceive() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(4);
		List<String> results = Collections.synchronizedList(new ArrayList<>());
		Stream.of(1, 2, 3, 4).forEach(i -> exec.execute(() -> {
			template.convertAndSend(ROUTE, "message");
			results.add((String) template.receiveAndConvert(ROUTE, 10_000L));
			latch.countDown();
		}));
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(results).contains("message", "message", "message", "message");
	}

	@RepeatedTest(50)
	public void testSendAndReceiveTransacted() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(4);
		List<String> results = Collections.synchronizedList(new ArrayList<>());
		template.setChannelTransacted(true);
		Stream.of(1, 2, 3, 4).forEach(i -> exec.execute(() -> {
			template.convertAndSend(ROUTE, "message");
			results.add((String) template.receiveAndConvert(ROUTE, 10_000L));
			latch.countDown();
		}));
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(results).contains("message", "message", "message", "message");
	}

	@RepeatedTest(50)
	public void testSendAndReceiveExternalTransacted() throws InterruptedException {
		template.setChannelTransacted(true);
		CountDownLatch latch = new CountDownLatch(4);
		List<String> results = Collections.synchronizedList(new ArrayList<>());
		template.setChannelTransacted(true);
		Stream.of(1, 2, 3, 4).forEach(i -> exec.execute(() -> {
			new TransactionTemplate(new TestTransactionManager())
					.executeWithoutResult(status -> template.convertAndSend(ROUTE, "message"));
			template.convertAndSend(ROUTE, "message");
			results.add((String) template.receiveAndConvert(ROUTE, 10_000L));
			latch.countDown();
		}));
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(results).contains("message", "message", "message", "message");
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		TestTransactionManager() {
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
