/*
 * Copyright 2010-2016 the original author or authors.
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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.ListenerContainerParserTests.TestBean;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 * @author Gary Russell
 */
public final class ListenerContainerPlaceholderParserTests {

	private GenericApplicationContext context;

	@Before
	public void setUp() throws Exception {
		this.context = new GenericXmlApplicationContext(
				new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@After
	public void closeBeanFactory() throws Exception {
		if (this.context != null) {
			CachingConnectionFactory cf = this.context.getBean(CachingConnectionFactory.class);
			this.context.close();
			assertTrue(TestUtils.getPropertyValue(cf, "deferredCloseExecutor", ThreadPoolExecutor.class)
					.isTerminated());
		}
	}

	@Test
	public void testParseWithQueueNames() throws Exception {
		SimpleMessageListenerContainer container = this.context.getBean("testListener", SimpleMessageListenerContainer.class);
		assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
		assertEquals(this.context.getBean(ConnectionFactory.class), container.getConnectionFactory());
		assertEquals(MessageListenerAdapter.class, container.getMessageListener().getClass());
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertEquals(this.context.getBean(TestBean.class), listenerAccessor.getPropertyValue("delegate"));
		assertEquals("handle", listenerAccessor.getPropertyValue("defaultListenerMethod"));
		Queue queue = this.context.getBean("bar", Queue.class);
		assertEquals("[foo, " + queue.getName() + "]", Arrays.asList(container.getQueueNames()).toString());
	}

}
