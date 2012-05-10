/*
 * Copyright 2002-2008 the original author or authors.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

public final class RabbitNamespaceHandlerTests {

	private XmlBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName()+"-context.xml", getClass()));
	}

	@Test
	public void testQueue() throws Exception {
		Queue queue = beanFactory.getBean("foo", Queue.class);
		assertNotNull(queue);
		assertEquals("foo", queue.getName());
	}

	@Test
	public void testAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("spam", Queue.class);
		assertNotNull(queue);
		assertNotSame("spam", queue.getName());
		assertEquals("bar", queue.getName());
	}

	@Test
	public void testAnonymousQueue() throws Exception {
		Queue queue = beanFactory.getBean("bucket", Queue.class);
		assertNotNull(queue);
		assertNotSame("bucket", queue.getName());
		assertTrue(queue instanceof AnonymousQueue);
	}

	@Test
	public void testExchanges() throws Exception {
		assertNotNull(beanFactory.getBean("direct-test", DirectExchange.class));
		assertNotNull(beanFactory.getBean("topic-test", TopicExchange.class));
		assertNotNull(beanFactory.getBean("fanout-test", FanoutExchange.class));
		assertNotNull(beanFactory.getBean("headers-test", HeadersExchange.class));
	}

	@Test
	public void testBindings() throws Exception {
		Map<String, Binding> bindings = beanFactory.getBeansOfType(Binding.class);
		// 4 for each exchange type
		assertEquals(17, bindings.size());
	}
	
	@Test
	public void testAdmin() throws Exception {
		assertNotNull(beanFactory.getBean("admin-test", RabbitAdmin.class));
	}

}
