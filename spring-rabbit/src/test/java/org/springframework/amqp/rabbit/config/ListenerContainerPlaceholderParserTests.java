/*
 * Copyright 2010-2013 the original author or authors.
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

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.ListenerContainerParserTests.TestBean;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 */
public final class ListenerContainerPlaceholderParserTests {

	private BeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new GenericXmlApplicationContext(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
		((GenericXmlApplicationContext)beanFactory).getDefaultListableBeanFactory().setBeanExpressionResolver(new StandardBeanExpressionResolver());
	}

	@After
	public void closeBeanFactory() throws Exception {
		if (beanFactory!=null) {
			((ConfigurableApplicationContext)beanFactory).close();
		}
	}

	@Test
	public void testParseWithQueueNames() throws Exception {
		SimpleMessageListenerContainer container = beanFactory.getBean("container1$testListener", SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
		assertEquals(beanFactory.getBean(ConnectionFactory.class), container.getConnectionFactory());
		assertEquals(MessageListenerAdapter.class, container.getMessageListener().getClass());
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertEquals(beanFactory.getBean(TestBean.class), listenerAccessor.getPropertyValue("delegate"));
		assertEquals("handle", listenerAccessor.getPropertyValue("defaultListenerMethod"));
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertEquals("[foo, "+queue.getName()+"]", Arrays.asList(container.getQueueNames()).toString());
	}

}
