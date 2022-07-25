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

package org.springframework.amqp.rabbit.config;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
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
 * @author Gary Russell
 * @author Sud Ramasamy
 *
 */
public class RabbitListenerContainerFactoryTests {

	private final SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();

	private final DirectRabbitListenerContainerFactory direct = new DirectRabbitListenerContainerFactory();

	private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

	private final ErrorHandler errorHandler = mock(ErrorHandler.class);

	private final MessageConverter messageConverter = new SimpleMessageConverter();

	private final MessageListenerAdapter messageListener = new MessageListenerAdapter();

	@Test
	public void createSimpleContainer() {
		setBasicConfig(this.factory);
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");
		BatchingStrategy bs1 = mock(BatchingStrategy.class);
		this.factory.setBatchingStrategy(bs1);
		BatchingStrategy bs2 = mock(BatchingStrategy.class);
		endpoint.setBatchingStrategy(bs2);

		SimpleMessageListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertBasicConfig(container);
		assertThat(container.getMessageListener()).isEqualTo(messageListener);
		assertThat(container.getQueueNames()[0]).isEqualTo("myQueue");
		assertThat(TestUtils.getPropertyValue(container, "batchingStrategy")).isSameAs(bs2);
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
		this.factory.setBatchSize(10);
		BatchingStrategy bs1 = mock(BatchingStrategy.class);
		this.factory.setBatchingStrategy(bs1);
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
		this.factory.setGlobalQos(true);
		this.factory.setContainerCustomizer(c -> c.setShutdownTimeout(10_000));

		assertThat(this.factory.getAdviceChain()).isEqualTo(new Advice[]{advice});

		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();

		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");
		SimpleMessageListenerContainer container = this.factory.createListenerContainer(endpoint);

		assertBasicConfig(container);
		assertThat(TestUtils.getPropertyValue(container, "batchingStrategy")).isSameAs(bs1);
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertThat(fieldAccessor.getPropertyValue("taskExecutor")).isSameAs(executor);
		assertThat(fieldAccessor.getPropertyValue("transactionManager")).isSameAs(transactionManager);
		assertThat(fieldAccessor.getPropertyValue("batchSize")).isEqualTo(10);
		assertThat(fieldAccessor.getPropertyValue("concurrentConsumers")).isEqualTo(2);
		assertThat(fieldAccessor.getPropertyValue("maxConcurrentConsumers")).isEqualTo(5);
		assertThat(fieldAccessor.getPropertyValue("startConsumerMinInterval")).isEqualTo(2000L);
		assertThat(fieldAccessor.getPropertyValue("stopConsumerMinInterval")).isEqualTo(2500L);
		assertThat(fieldAccessor.getPropertyValue("consecutiveActiveTrigger")).isEqualTo(8);
		assertThat(fieldAccessor.getPropertyValue("consecutiveIdleTrigger")).isEqualTo(6);
		assertThat(fieldAccessor.getPropertyValue("prefetchCount")).isEqualTo(3);
		assertThat(fieldAccessor.getPropertyValue("receiveTimeout")).isEqualTo(1500L);
		assertThat(fieldAccessor.getPropertyValue("shutdownTimeout")).isEqualTo(10_000L);
		assertThat(fieldAccessor.getPropertyValue("defaultRequeueRejected")).isEqualTo(false);
		Advice[] actualAdviceChain = (Advice[]) fieldAccessor.getPropertyValue("adviceChain");
		assertThat(actualAdviceChain.length).as("Wrong number of advice").isEqualTo(1);
		assertThat(actualAdviceChain[0]).as("Wrong advice").isSameAs(advice);
		assertThat(fieldAccessor.getPropertyValue("recoveryBackOff")).isSameAs(recoveryBackOff);
		assertThat(fieldAccessor.getPropertyValue("missingQueuesFatal")).isEqualTo(true);
		assertThat(container.getMessageListener()).isEqualTo(messageListener);
		assertThat(container.getQueueNames()[0]).isEqualTo("myQueue");
		List<?> actualAfterReceivePostProcessors = (List<?>) fieldAccessor.getPropertyValue("afterReceivePostProcessors");
		assertThat(actualAfterReceivePostProcessors.size()).as("Wrong number of afterReceivePostProcessors").isEqualTo(1);
		assertThat(actualAfterReceivePostProcessors.get(0)).as("Wrong advice").isSameAs(afterReceivePostProcessor);
		assertThat(fieldAccessor.getPropertyValue("globalQos")).isEqualTo(true);
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
		this.direct.setMessagesPerAck(5);
		this.direct.setAckTimeout(3L);
		this.direct.setAfterReceivePostProcessors(afterReceivePostProcessor);

		assertThat(this.direct.getAdviceChain()).isEqualTo(new Advice[]{advice});

		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();

		endpoint.setMessageListener(this.messageListener);
		endpoint.setQueueNames("myQueue");
		DirectMessageListenerContainer container = this.direct.createListenerContainer(endpoint);

		assertBasicConfig(container);
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertThat(fieldAccessor.getPropertyValue("taskExecutor")).isSameAs(executor);
		assertThat(fieldAccessor.getPropertyValue("transactionManager")).isSameAs(transactionManager);
		assertThat(fieldAccessor.getPropertyValue("prefetchCount")).isEqualTo(3);
		assertThat(fieldAccessor.getPropertyValue("defaultRequeueRejected")).isEqualTo(false);
		Advice[] actualAdviceChain = (Advice[]) fieldAccessor.getPropertyValue("adviceChain");
		assertThat(actualAdviceChain.length).as("Wrong number of advice").isEqualTo(1);
		assertThat(actualAdviceChain[0]).as("Wrong advice").isSameAs(advice);
		assertThat(fieldAccessor.getPropertyValue("recoveryBackOff")).isSameAs(recoveryBackOff);
		assertThat(fieldAccessor.getPropertyValue("missingQueuesFatal")).isEqualTo(true);
		assertThat(fieldAccessor.getPropertyValue("mismatchedQueuesFatal")).isEqualTo(false);
		assertThat(container.getMessageListener()).isEqualTo(messageListener);
		assertThat(container.getQueueNames()[0]).isEqualTo("myQueue");
		assertThat(fieldAccessor.getPropertyValue("taskScheduler")).isSameAs(scheduler);
		assertThat(fieldAccessor.getPropertyValue("monitorInterval")).isEqualTo(1234L);
		assertThat(fieldAccessor.getPropertyValue("consumersPerQueue")).isEqualTo(42);
		assertThat(fieldAccessor.getPropertyValue("messagesPerAck")).isEqualTo(5);
		assertThat(fieldAccessor.getPropertyValue("ackTimeout")).isEqualTo(3L);
		List<?> actualAfterReceivePostProcessors = (List<?>) fieldAccessor.getPropertyValue("afterReceivePostProcessors");
		assertThat(actualAfterReceivePostProcessors.size()).as("Wrong number of afterReceivePostProcessors").isEqualTo(1);
		assertThat(actualAfterReceivePostProcessors.get(0)).as("Wrong afterReceivePostProcessor").isSameAs(afterReceivePostProcessor);
	}

	private void setBasicConfig(AbstractRabbitListenerContainerFactory<?> factory) {
		factory.setConnectionFactory(this.connectionFactory);
		factory.setErrorHandler(this.errorHandler);
		this.messageListener.setMessageConverter(this.messageConverter);
		factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		factory.setChannelTransacted(true);
		factory.setAutoStartup(false);
		factory.setPhase(99);
	}

	private void assertBasicConfig(AbstractMessageListenerContainer container) {
		DirectFieldAccessor fieldAccessor = new DirectFieldAccessor(container);
		assertThat(container.getConnectionFactory()).isSameAs(connectionFactory);
		assertThat(fieldAccessor.getPropertyValue("errorHandler")).isSameAs(errorHandler);
		assertThat(fieldAccessor.getPropertyValue("messageListener.messageConverter")).isSameAs(messageConverter);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.MANUAL);
		assertThat(container.isChannelTransacted()).isEqualTo(true);
		assertThat(container.isAutoStartup()).isEqualTo(false);
		assertThat(container.getPhase()).isEqualTo(99);
	}

}
