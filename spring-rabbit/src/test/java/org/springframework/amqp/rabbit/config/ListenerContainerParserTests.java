/*
 * Copyright 2010-2017 the original author or authors.
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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.aopalliance.aop.Advice;
import org.junit.Before;
import org.junit.Test;

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

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @author Artem Bilan
 */
public class ListenerContainerParserTests {

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
		beanFactory.setBeanExpressionResolver(new StandardBeanExpressionResolver());
	}

	@Test
	public void testParseWithQueueNames() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container1", SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
		assertEquals(beanFactory.getBean(ConnectionFactory.class), container.getConnectionFactory());
		assertEquals(MessageListenerAdapter.class, container.getMessageListener().getClass());
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertEquals(beanFactory.getBean(TestBean.class), listenerAccessor.getPropertyValue("delegate"));
		assertEquals("handle", listenerAccessor.getPropertyValue("defaultListenerMethod"));
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertEquals("[foo, " + queue.getName() + "]", Arrays.asList(container.getQueueNames()).toString());
		assertEquals(5, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		assertEquals(6, ReflectionTestUtils.getField(container, "maxConcurrentConsumers"));
		assertEquals(1234L, ReflectionTestUtils.getField(container, "startConsumerMinInterval"));
		assertEquals(2345L, ReflectionTestUtils.getField(container, "stopConsumerMinInterval"));
		assertEquals(12, ReflectionTestUtils.getField(container, "consecutiveActiveTrigger"));
		assertEquals(34, ReflectionTestUtils.getField(container, "consecutiveIdleTrigger"));
		assertEquals(9876L, ReflectionTestUtils.getField(container, "receiveTimeout"));
		Map<?, ?> consumerArgs = TestUtils.getPropertyValue(container, "consumerArgs", Map.class);
		assertEquals(1, consumerArgs.size());
		Object xPriority = consumerArgs.get("x-priority");
		assertNotNull(xPriority);
		assertEquals(10, xPriority);
		assertEquals(Long.valueOf(5555), TestUtils.getPropertyValue(container, "recoveryBackOff.interval", Long.class));
		assertFalse(TestUtils.getPropertyValue(container, "exclusive", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(container, "missingQueuesFatal", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(container, "possibleAuthenticationFailureFatal", Boolean.class));
		assertTrue(TestUtils.getPropertyValue(container, "autoDeclare", Boolean.class));
		assertEquals(5, TestUtils.getPropertyValue(container, "declarationRetries"));
		assertEquals(1000L, TestUtils.getPropertyValue(container, "failedDeclarationRetryInterval"));
		assertEquals(30000L, TestUtils.getPropertyValue(container, "retryDeclarationInterval"));
		assertEquals(beanFactory.getBean("tagger"), TestUtils.getPropertyValue(container, "consumerTagStrategy"));
		Collection<?> group = beanFactory.getBean("containerGroup", Collection.class);
		assertEquals(4, group.size());
		assertThat(group, contains(beanFactory.getBean("container1"), beanFactory.getBean("testListener1"),
				beanFactory.getBean("testListener2"), beanFactory.getBean("direct1")));
		assertEquals(1235L, ReflectionTestUtils.getField(container, "idleEventInterval"));
		assertEquals("container1", container.getListenerId());
		assertTrue(TestUtils.getPropertyValue(container, "mismatchedQueuesFatal", Boolean.class));
	}

	@Test
	public void testParseWithDirect() throws Exception {
		DirectMessageListenerContainer container = beanFactory.getBean("direct1", DirectMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
		assertEquals(beanFactory.getBean(ConnectionFactory.class), container.getConnectionFactory());
		assertEquals(MessageListenerAdapter.class, container.getMessageListener().getClass());
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertEquals(beanFactory.getBean(TestBean.class), listenerAccessor.getPropertyValue("delegate"));
		assertEquals("handle", listenerAccessor.getPropertyValue("defaultListenerMethod"));
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertEquals("[foo, " + queue.getName() + "]", Arrays.asList(container.getQueueNames()).toString());
		assertEquals(5, ReflectionTestUtils.getField(container, "consumersPerQueue"));
		assertEquals(5000L, ReflectionTestUtils.getField(container, "monitorInterval"));
		assertSame(this.beanFactory.getBean("sched"), ReflectionTestUtils.getField(container, "taskScheduler"));
		assertSame(this.beanFactory.getBean("exec"), ReflectionTestUtils.getField(container, "taskExecutor"));
		Map<?, ?> consumerArgs = TestUtils.getPropertyValue(container, "consumerArgs", Map.class);
		assertEquals(1, consumerArgs.size());
		Object xPriority = consumerArgs.get("x-priority");
		assertNotNull(xPriority);
		assertEquals(10, xPriority);
		assertEquals(Long.valueOf(5555), TestUtils.getPropertyValue(container, "recoveryBackOff.interval", Long.class));
		assertFalse(TestUtils.getPropertyValue(container, "exclusive", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(container, "missingQueuesFatal", Boolean.class));
		assertTrue(TestUtils.getPropertyValue(container, "autoDeclare", Boolean.class));
		assertEquals(1000L, TestUtils.getPropertyValue(container, "failedDeclarationRetryInterval"));
		assertEquals(beanFactory.getBean("tagger"), TestUtils.getPropertyValue(container, "consumerTagStrategy"));
		Collection<?> group = beanFactory.getBean("containerGroup", Collection.class);
		assertEquals(4, group.size());
		assertThat(group, contains(beanFactory.getBean("container1"), beanFactory.getBean("testListener1"),
				beanFactory.getBean("testListener2"), beanFactory.getBean("direct1")));
		assertEquals(1235L, ReflectionTestUtils.getField(container, "idleEventInterval"));
		assertEquals("direct1", container.getListenerId());
		assertTrue(TestUtils.getPropertyValue(container, "mismatchedQueuesFatal", Boolean.class));
	}

	@Test
	public void testParseWithQueues() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container2", SimpleMessageListenerContainer.class);
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertEquals("[foo, " + queue.getName() + "]", Arrays.asList(container.getQueueNames()).toString());
		assertTrue(TestUtils.getPropertyValue(container, "missingQueuesFatal", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(container, "autoDeclare", Boolean.class));
	}

	@Test
	public void testParseWithAdviceChain() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container3", SimpleMessageListenerContainer.class);
		Object adviceChain = ReflectionTestUtils.getField(container, "adviceChain");
		assertNotNull(adviceChain);
		assertEquals(3, ((Advice[]) adviceChain).length);
		assertTrue(TestUtils.getPropertyValue(container, "exclusive", Boolean.class));
	}

	@Test
	public void testParseWithDefaults() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container4", SimpleMessageListenerContainer.class);
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		assertEquals(true, ReflectionTestUtils.getField(container, "defaultRequeueRejected"));
	}

	@Test
	public void testParseWithDefaultQueueRejectedFalse() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container5", SimpleMessageListenerContainer.class);
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		assertEquals(false, ReflectionTestUtils.getField(container, "defaultRequeueRejected"));
		assertFalse(container.isChannelTransacted());
	}

	@Test
	public void testParseWithTx() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container6", SimpleMessageListenerContainer.class);
		assertTrue(container.isChannelTransacted());
		assertEquals(5, ReflectionTestUtils.getField(container, "txSize"));
	}

	@Test
	public void testNamedListeners() throws Exception {
		beanFactory.getBean("testListener1", SimpleMessageListenerContainer.class);
		beanFactory.getBean("testListener2", SimpleMessageListenerContainer.class);
	}

	@Test
	public void testAnonListeners() throws Exception {
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#0",
				SimpleMessageListenerContainer.class);
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#1",
				SimpleMessageListenerContainer.class);
		beanFactory.getBean("namedListener", SimpleMessageListenerContainer.class);
		beanFactory.getBean("org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#2",
				SimpleMessageListenerContainer.class);
	}

	@Test
	public void testAnonEverything() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean(
				"org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#3",
				SimpleMessageListenerContainer.class);
		assertEquals("ex1", ReflectionTestUtils.getField(ReflectionTestUtils.getField(container, "messageListener"),
				"responseExchange"));
		container = beanFactory.getBean(
				"org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean#4",
				SimpleMessageListenerContainer.class);
		assertEquals("ex2", ReflectionTestUtils.getField(ReflectionTestUtils.getField(container, "messageListener"),
				"responseExchange"));
	}

	@Test
	public void testAnonParent() throws Exception {
		beanFactory.getBean("anonParentL1", SimpleMessageListenerContainer.class);
		beanFactory.getBean("anonParentL2", SimpleMessageListenerContainer.class);
	}

	@Test
	public void testIncompatibleTxAtts() {
		try {
			new ClassPathXmlApplicationContext(getClass().getSimpleName() + "-fail-context.xml", getClass()).close();
			fail("Parse exception expected");
		}
		catch (BeanDefinitionParsingException e) {
			assertTrue(e.getMessage().startsWith(
				"Configuration problem: Listener Container - cannot set channel-transacted with acknowledge='NONE'"));
		}
	}

	static class TestBean {

		public void handle(String s) {
		}

	}

	static class TestAdvice implements MethodBeforeAdvice {

		@Override
		public void before(Method method, Object[] args, Object target) throws Throwable {
		}

	}

	public static class TestConsumerTagStrategy implements ConsumerTagStrategy {

		@Override
		public String createConsumerTag(String queue) {
			return "foo";
		}

	}

}
