/*
 * Copyright 2002-2008 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

public class QueueParserTests {

	protected BeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testQueue() throws Exception {
		Queue queue = beanFactory.getBean("foo", Queue.class);
		assertNotNull(queue);
		assertEquals("foo", queue.getName());
		assertTrue(queue.isDurable());
		assertFalse(queue.isAutoDelete());
		assertFalse(queue.isExclusive());
	}

	@Test
	public void testAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("alias", Queue.class);
		assertNotNull(queue);
		assertEquals("spam", queue.getName());
		assertNotSame("alias", queue.getName());
	}

	@Test
	public void testOverrideQueue() throws Exception {
		Queue queue = beanFactory.getBean("override", Queue.class);
		assertNotNull(queue);
		assertEquals("override", queue.getName());
		assertTrue(queue.isDurable());
		assertTrue(queue.isExclusive());
		assertTrue(queue.isAutoDelete());
	}

	@Test
	public void testOverrideAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("overrideAlias", Queue.class);
		assertNotNull(queue);
		assertEquals("bar", queue.getName());
		assertTrue(queue.isDurable());
		assertTrue(queue.isExclusive());
		assertTrue(queue.isAutoDelete());
	}

	@Test
	public void testAnonymousQueue() throws Exception {
		Queue queue = beanFactory.getBean("anonymous", Queue.class);
		assertNotNull(queue);
		assertNotSame("anonymous", queue.getName());
		assertTrue(queue instanceof AnonymousQueue);
		assertFalse(queue.isDurable());
		assertTrue(queue.isExclusive());
		assertTrue(queue.isAutoDelete());
	}

	@Test
	public void testArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("arguments", Queue.class);
		assertNotNull(queue);
		assertEquals("bar", queue.getArguments().get("foo"));
	}

	@Test
	public void testAnonymousArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("anonymousArguments", Queue.class);
		assertNotNull(queue);
		assertEquals("spam", queue.getArguments().get("foo"));
	}

	@Test
	public void testReferencedArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("referencedArguments", Queue.class);
		assertNotNull(queue);
		assertEquals("qux", queue.getArguments().get("baz"));
	}

	@Test(expected=BeanDefinitionStoreException.class)
	public void testIllegalAnonymousQueue() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName()
				+ "IllegalAnonymous-context.xml", getClass()));
		Queue queue = beanFactory.getBean("anonymous", Queue.class);
		assertNotNull(queue);
		assertNotSame("bucket", queue.getName());
		assertTrue(queue instanceof AnonymousQueue);
	}

}
