/*
 * Copyright 2010-2011 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.aopalliance.aop.Advice;
import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.aop.MethodBeforeAdvice;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Mark Fisher
 */
public final class ListenerContainerParserTests {

	private XmlBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
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
		assertEquals("[foo, "+queue.getName()+"]", Arrays.asList(container.getQueueNames()).toString());
	}

	@Test
	public void testParseWithQueues() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container2", SimpleMessageListenerContainer.class);
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertEquals("[foo, "+queue.getName()+"]", Arrays.asList(container.getQueueNames()).toString());
	}

	@Test
	public void testParseWithAdviceChain() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container3", SimpleMessageListenerContainer.class);
		Object adviceChain = ReflectionTestUtils.getField(container, "adviceChain");
		assertNotNull(adviceChain);
		assertEquals(3, ((Advice[]) adviceChain).length);
	}

	@Test
	public void testParseWithDefaults() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container4", SimpleMessageListenerContainer.class);
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
	}

	static class TestBean {
		public void handle(String s) {
		}
	}

	static class TestAdvice implements MethodBeforeAdvice {
		public void before(Method method, Object[] args, Object target) throws Throwable {
		}
	}
}
