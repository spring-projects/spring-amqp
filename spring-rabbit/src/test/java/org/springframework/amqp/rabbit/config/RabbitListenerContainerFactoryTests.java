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

package org.springframework.amqp.rabbit.config;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Joris Kuipers
 *
 */
public class RabbitListenerContainerFactoryTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private final SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();

	private final DirectRabbitListenerContainerFactory direct = new DirectRabbitListenerContainerFactory();

	private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

	private final ErrorHandler errorHandler = mock(ErrorHandler.class);

	private final MessageConverter messageConverter = new SimpleMessageConverter();

	private final MessageListener messageListener = new MessageListenerAdapter();

	@Test
	public void createSimpleContainer() {
		setBasicConfig(this.factory);
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");

		SimpleMessageListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertBasicConfig(container);
		assertEquals(messageListener, container.getMessageListener());
		assertEquals("myQueue", container.getQueueNames()[0]);
	}

	@Test
	public void createContainerFullConfig() {
		Executor executor = mock(Executor.class);
		PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
		Advice advice = mock(Advice.class);
		MessagePostProcessor afterReceivePostProcessor = mock(MessagePostProcessor.class);

		setBasicConfig(this.factory);
		this.factory.setTaskExecutor(executor);
		this.factory.setTransactionManager(transactionManager);
		this.factory.setTxSize(10);
		this.factory.setConcurrentConsumers(2);
		this.factory.setMaxConcurrentConsumers(5);
		this.factory.setStartConsumerMinInterval(2000L);
		this.factory.setStopConsumerMinInterval(2500L);
		this.factory.setConsecutiveActiveTrigger(8);
		this.factory.setConsecutiveIdleTrigger(6);
		this.factory.setPrefetchCount(3);
		this.factory.setReceiveTimeout(1500L);
		this.factory.setDefaultRequeueRejected(false);
		this.factory.setAdviceChain(advice);
		BackOff recoveryBackOff = new ExponentialBackOff();
		this.factory.setRecoveryBackOff(recoveryBackOff);
		this.factory.setMissingQueuesFatal(true);
		this.factory.setAfterReceivePostProcessors(afterReceivePostProcessor);

		assertArrayEquals(new Advice[] {advice}, this.factory.getAdviceChain());

		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();

		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");
		SimpleMessageListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertBasicConfig(container);
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertSame(executor, fieldAccessor.getPropertyValue("taskExecutor"));
		assertSame(transactionManager, fieldAccessor.getPropertyValue("transactionManager"));
		assertEquals(10, fieldAccessor.getPropertyValue("txSize"));
		assertEquals(2, fieldAccessor.getPropertyValue("concurrentConsumers"));
		assertEquals(5, fieldAccessor.getPropertyValue("maxConcurrentConsumers"));
		assertEquals(2000L, fieldAccessor.getPropertyValue("startConsumerMinInterval"));
		assertEquals(2500L, fieldAccessor.getPropertyValue("stopConsumerMinInterval"));
		assertEquals(8, fieldAccessor.getPropertyValue("consecutiveActiveTrigger"));
		assertEquals(6, fieldAccessor.getPropertyValue("consecutiveIdleTrigger"));
		assertEquals(3, fieldAccessor.getPropertyValue("prefetchCount"));
		assertEquals(1500L, fieldAccessor.getPropertyValue("receiveTimeout"));
		assertEquals(false, fieldAccessor.getPropertyValue("defaultRequeueRejected"));
		Advice[] actualAdviceChain = (Advice[]) fieldAccessor.getPropertyValue("adviceChain");
		assertEquals("Wrong number of advice", 1, actualAdviceChain.length);
		assertSame("Wrong advice", advice, actualAdviceChain[0]);
		assertSame(recoveryBackOff, fieldAccessor.getPropertyValue("recoveryBackOff"));
		assertEquals(true, fieldAccessor.getPropertyValue("missingQueuesFatal"));
		assertEquals(messageListener, container.getMessageListener());
		assertEquals("myQueue", container.getQueueNames()[0]);
		List<?> actualAfterReceivePostProcessors = (List<?>) fieldAccessor.getPropertyValue("afterReceivePostProcessors");
		assertEquals("Wrong number of afterReceivePostProcessors", 1, actualAfterReceivePostProcessors.size());
		assertSame("Wrong advice", afterReceivePostProcessor, actualAfterReceivePostProcessors.get(0));
	}

	@Test
	public void createDirectContainerFullConfig() {
		Executor executor = mock(Executor.class);
		TaskScheduler scheduler = mock(TaskScheduler.class);
		PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
		Advice advice = mock(Advice.class);
		MessagePostProcessor afterReceivePostProcessor = mock(MessagePostProcessor.class);

		setBasicConfig(this.direct);
		this.direct.setTaskExecutor(executor);
		this.direct.setTransactionManager(transactionManager);
		this.direct.setPrefetchCount(3);
		this.direct.setDefaultRequeueRejected(false);
		this.direct.setAdviceChain(advice);
		BackOff recoveryBackOff = new ExponentialBackOff();
		this.direct.setRecoveryBackOff(recoveryBackOff);
		this.direct.setMissingQueuesFatal(true);
		this.direct.setMismatchedQueuesFatal(false);
		this.direct.setTaskScheduler(scheduler);
		this.direct.setMonitorInterval(1234L);
		this.direct.setConsumersPerQueue(42);
		this.direct.setAfterReceivePostProcessors(afterReceivePostProcessor);

		assertArrayEquals(new Advice[] {advice}, this.direct.getAdviceChain());

		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();

		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");
		DirectMessageListenerContainer container = this.direct.createListenerContainer(endpoint);

		assertBasicConfig(container);
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertSame(executor, fieldAccessor.getPropertyValue("taskExecutor"));
		assertSame(transactionManager, fieldAccessor.getPropertyValue("transactionManager"));
		assertEquals(3, fieldAccessor.getPropertyValue("prefetchCount"));
		assertEquals(false, fieldAccessor.getPropertyValue("defaultRequeueRejected"));
		Advice[] actualAdviceChain = (Advice[]) fieldAccessor.getPropertyValue("adviceChain");
		assertEquals("Wrong number of advice", 1, actualAdviceChain.length);
		assertSame("Wrong advice", advice, actualAdviceChain[0]);
		assertSame(recoveryBackOff, fieldAccessor.getPropertyValue("recoveryBackOff"));
		assertEquals(true, fieldAccessor.getPropertyValue("missingQueuesFatal"));
		assertEquals(false, fieldAccessor.getPropertyValue("mismatchedQueuesFatal"));
		assertEquals(messageListener, container.getMessageListener());
		assertEquals("myQueue", container.getQueueNames()[0]);
		assertSame(scheduler, fieldAccessor.getPropertyValue("taskScheduler"));
		assertEquals(1234L, fieldAccessor.getPropertyValue("monitorInterval"));
		assertEquals(42, fieldAccessor.getPropertyValue("consumersPerQueue"));
		List<?> actualAfterReceivePostProcessors = (List<?>) fieldAccessor.getPropertyValue("afterReceivePostProcessors");
		assertEquals("Wrong number of afterReceivePostProcessors", 1, actualAfterReceivePostProcessors.size());
		assertSame("Wrong afterReceivePostProcessor", afterReceivePostProcessor, actualAfterReceivePostProcessors.get(0));
	}

	private void setBasicConfig(AbstractRabbitListenerContainerFactory<?> factory) {
		factory.setConnectionFactory(this.connectionFactory);
		factory.setErrorHandler(this.errorHandler);
		factory.setMessageConverter(this.messageConverter);
		factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		factory.setChannelTransacted(true);
		factory.setAutoStartup(false);
		factory.setPhase(99);
	}

	private void assertBasicConfig(AbstractMessageListenerContainer container) {
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertSame(connectionFactory, container.getConnectionFactory());
		assertSame(errorHandler, fieldAccessor.getPropertyValue("errorHandler"));
		assertSame(messageConverter, container.getMessageConverter());
		assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
		assertEquals(true, container.isChannelTransacted());
		assertEquals(false, container.isAutoStartup());
		assertEquals(99, container.getPhase());
	}

}
