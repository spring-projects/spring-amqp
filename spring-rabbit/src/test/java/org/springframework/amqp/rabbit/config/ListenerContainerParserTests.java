/*
 * Copyright 2010-present the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.aopalliance.aop.Advice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.MethodBeforeAdvice;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 */
public class ListenerContainerParserTests {

	private DefaultListableBeanFactory beanFactory;

	@BeforeEach
	public void setUp() {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
		beanFactory.setBeanExpressionResolver(new StandardBeanExpressionResolver());
	}

	@Test
	public void testParseWithQueueNames() {
		SimpleMessageListenerContainer container =
				this.beanFactory.getBean("container1", SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.MANUAL);
		assertThat(container.getConnectionFactory()).isEqualTo(beanFactory.getBean(ConnectionFactory.class));
		assertThat(container.getMessageListener().getClass()).isEqualTo(MessageListenerAdapter.class);
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertThat(listenerAccessor.getPropertyValue("delegate")).isEqualTo(beanFactory.getBean(TestBean.class));
		assertThat(listenerAccessor.getPropertyValue("defaultListenerMethod")).isEqualTo("handle");
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertThat(Arrays.asList(container.getQueueNames()).toString()).isEqualTo("[foo, " + queue.getName() + "]");
		assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(5);
		assertThat(ReflectionTestUtils.getField(container, "maxConcurrentConsumers")).isEqualTo(6);
		assertThat(ReflectionTestUtils.getField(container, "startConsumerMinInterval")).isEqualTo(1234L);
		assertThat(ReflectionTestUtils.getField(container, "stopConsumerMinInterval")).isEqualTo(2345L);
		assertThat(ReflectionTestUtils.getField(container, "consecutiveActiveTrigger")).isEqualTo(12);
		assertThat(ReflectionTestUtils.getField(container, "consecutiveIdleTrigger")).isEqualTo(34);
		assertThat(ReflectionTestUtils.getField(container, "receiveTimeout")).isEqualTo(9876L);
		Map<?, ?> consumerArgs = TestUtils.propertyValue(container, "consumerArgs");
		assertThat(consumerArgs).hasSize(1);
		Object xPriority = consumerArgs.get("x-priority");
		assertThat(xPriority).isEqualTo(10);
		assertThat(TestUtils.getPropertyValue(container, "recoveryBackOff.interval")).isEqualTo(5555L);
		assertThat(TestUtils.<Boolean>propertyValue(container, "exclusive")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "missingQueuesFatal")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "possibleAuthenticationFailureFatal")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "autoDeclare")).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "declarationRetries")).isEqualTo(5);
		assertThat(TestUtils.getPropertyValue(container, "failedDeclarationRetryInterval")).isEqualTo(1000L);
		assertThat(TestUtils.getPropertyValue(container, "retryDeclarationInterval")).isEqualTo(30000L);
		assertThat(TestUtils.getPropertyValue(container, "consumerTagStrategy")).isEqualTo(beanFactory.getBean("tagger"));
		@SuppressWarnings("unchecked")
		Collection<Object> group = beanFactory.getBean("containerGroup", Collection.class);
		assertThat(group).hasSize(4);
		assertThat(group).containsExactly(beanFactory.getBean("container1"), beanFactory.getBean("testListener1"),
				beanFactory.getBean("testListener2"), beanFactory.getBean("direct1"));
		assertThat(ReflectionTestUtils.getField(container, "idleEventInterval")).isEqualTo(1235L);
		assertThat(container.getListenerId()).isEqualTo("container1");
		assertThat(TestUtils.<Boolean>propertyValue(container, "mismatchedQueuesFatal")).isTrue();
		assertThat(TestUtils.<Boolean>propertyValue(container, "globalQos")).isFalse();
	}

	@Test
	public void testParseWithDirect() {
		DirectMessageListenerContainer container = beanFactory.getBean("direct1", DirectMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.MANUAL);
		assertThat(container.getConnectionFactory()).isEqualTo(beanFactory.getBean(ConnectionFactory.class));
		assertThat(container.getMessageListener().getClass()).isEqualTo(MessageListenerAdapter.class);
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertThat(listenerAccessor.getPropertyValue("delegate")).isEqualTo(beanFactory.getBean(TestBean.class));
		assertThat(listenerAccessor.getPropertyValue("defaultListenerMethod")).isEqualTo("handle");
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertThat(Arrays.asList(container.getQueueNames()).toString()).isEqualTo("[foo, " + queue.getName() + "]");
		assertThat(ReflectionTestUtils.getField(container, "consumersPerQueue")).isEqualTo(5);
		assertThat(ReflectionTestUtils.getField(container, "monitorInterval")).isEqualTo(5000L);
		assertThat(ReflectionTestUtils.getField(container, "taskScheduler")).isSameAs(this.beanFactory.getBean("sched"));
		assertThat(ReflectionTestUtils.getField(container, "taskExecutor")).isSameAs(this.beanFactory.getBean("exec"));
		Map<?, ?> consumerArgs = TestUtils.propertyValue(container, "consumerArgs");
		assertThat(consumerArgs).hasSize(1);
		Object xPriority = consumerArgs.get("x-priority");
		assertThat(xPriority).isEqualTo(10);
		assertThat(TestUtils.getPropertyValue(container, "recoveryBackOff.interval")).isEqualTo(5555L);
		assertThat(TestUtils.<Boolean>propertyValue(container, "exclusive")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "missingQueuesFatal")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "autoDeclare")).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "failedDeclarationRetryInterval")).isEqualTo(1000L);
		assertThat(TestUtils.getPropertyValue(container, "consumerTagStrategy")).isEqualTo(beanFactory.getBean("tagger"));
		@SuppressWarnings("unchecked")
		Collection<Object> group = beanFactory.getBean("containerGroup", Collection.class);
		assertThat(group).hasSize(4);
		assertThat(group).containsExactly(beanFactory.getBean("container1"), beanFactory.getBean("testListener1"),
				beanFactory.getBean("testListener2"), beanFactory.getBean("direct1"));
		assertThat(ReflectionTestUtils.getField(container, "idleEventInterval")).isEqualTo(1235L);
		assertThat(container.getListenerId()).isEqualTo("direct1");
		assertThat(TestUtils.<Boolean>propertyValue(container, "mismatchedQueuesFatal")).isTrue();
	}

	@Test
	public void testParseWithQueues() {
		SimpleMessageListenerContainer container = beanFactory.getBean("container2", SimpleMessageListenerContainer.class);
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertThat(Arrays.asList(container.getQueueNames()).toString()).isEqualTo("[foo, " + queue.getName() + "]");
		assertThat(TestUtils.<Boolean>propertyValue(container, "missingQueuesFatal")).isTrue();
		assertThat(TestUtils.<Boolean>propertyValue(container, "autoDeclare")).isFalse();
		assertThat(TestUtils.<Boolean>propertyValue(container, "globalQos")).isTrue();
	}

	@Test
	public void testParseWithAdviceChain() {
		SimpleMessageListenerContainer container = beanFactory.getBean("container3", SimpleMessageListenerContainer.class);
		Advice[] adviceChain = TestUtils.propertyValue(container, "adviceChain");
		assertThat(adviceChain).hasSize(3);
		assertThat(TestUtils.<Boolean>propertyValue(container, "exclusive")).isTrue();
	}

	@Test
	public void testParseWithDefaults() {
		SimpleMessageListenerContainer container = beanFactory.getBean("container4", SimpleMessageListenerContainer.class);
		assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(1);
		assertThat(ReflectionTestUtils.getField(container, "defaultRequeueRejected")).isEqualTo(true);
	}

	@Test
	public void testParseWithDefaultQueueRejectedFalse() {
		SimpleMessageListenerContainer container = beanFactory.getBean("container5", SimpleMessageListenerContainer.class);
		assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(1);
		assertThat(ReflectionTestUtils.getField(container, "defaultRequeueRejected")).isEqualTo(false);
		assertThat(container.isChannelTransacted()).isFalse();
	}

	@Test
	public void testParseWithTx() {
		SimpleMessageListenerContainer container = beanFactory.getBean("container6", SimpleMessageListenerContainer.class);
		assertThat(container.isChannelTransacted()).isTrue();
		assertThat(ReflectionTestUtils.getField(container, "batchSize")).isEqualTo(5);
		assertThat(ReflectionTestUtils.getField(container, "consumerBatchEnabled")).isEqualTo(Boolean.TRUE);
	}

	@Test
	public void testNamedListeners() {
		assertThatNoException()
				.isThrownBy(() -> {
					beanFactory.getBean("testListener1", SimpleMessageListenerContainer.class);
					beanFactory.getBean("testListener2", SimpleMessageListenerContainer.class);
				});
	}

	@Test
	public void testAnonListeners() {
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#0",
				SimpleMessageListenerContainer.class);
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#1",
				SimpleMessageListenerContainer.class);
		beanFactory.getBean("namedListener", SimpleMessageListenerContainer.class);
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#2",
				SimpleMessageListenerContainer.class);
	}

	@Test
	public void testAnonEverything() {
		SimpleMessageListenerContainer container = beanFactory.getBean(
				"org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#3",
				SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(container, "messageListener.responseExchange")).isEqualTo("ex1");
		container = beanFactory.getBean(
				"org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#4",
				SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(container, "messageListener.responseExchange")).isEqualTo("ex2");
	}

	@Test
	public void testAnonParent() {
		beanFactory.getBean("anonParentL1", SimpleMessageListenerContainer.class);
		beanFactory.getBean("anonParentL2", SimpleMessageListenerContainer.class);
	}

	@Test
	public void testIncompatibleTxAtts() {

		String path = getClass().getSimpleName() + "-fail-context.xml";
		assertThatExceptionOfType(BeanDefinitionParsingException.class)
				.isThrownBy(() -> new ClassPathXmlApplicationContext(path, getClass()).close())
				.withMessageStartingWith(
						"Configuration problem: Listener Container - " +
								"cannot set channel-transacted with acknowledge='NONE'");
	}

	@Test
	public void testParseMessagePostProcessor() {
		SimpleMessageListenerContainer listenerContainer =
				this.beanFactory.getBean("testMessagePostProcessor", SimpleMessageListenerContainer.class);

		Collection<Object> messagePostProcessors =
				TestUtils.propertyValue(listenerContainer, "afterReceivePostProcessors");

		assertThat(messagePostProcessors).isNotEmpty()
				.containsExactly(this.beanFactory.getBean("unzipPostProcessor"),
						this.beanFactory.getBean("gUnzipPostProcessor"));
	}

	static class TestBean {

		public void handle(String s) {
		}

	}

	static class TestAdvice implements MethodBeforeAdvice {

		@Override
		public void before(Method method, Object[] args, Object target) {
		}

	}

	public static class TestConsumerTagStrategy implements ConsumerTagStrategy {

		@Override
		public String createConsumerTag(String queue) {
			return "foo";
		}

	}

}
