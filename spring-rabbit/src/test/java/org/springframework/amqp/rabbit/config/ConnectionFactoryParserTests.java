/*
 * Copyright 2002-2016 the original author or authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;

/**
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public final class ConnectionFactoryParserTests {

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testKitchenSink() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("kitchenSink", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		assertNull(dfa.getPropertyValue("executorService"));
		assertEquals(Boolean.TRUE, dfa.getPropertyValue("publisherConfirms"));
		assertEquals(Boolean.TRUE, dfa.getPropertyValue("publisherReturns"));
		assertEquals(123, TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.requestedHeartbeat"));
		assertEquals(789,  TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.connectionTimeout"));
		assertEquals(CachingConnectionFactory.CacheMode.CHANNEL, connectionFactory.getCacheMode());
		assertEquals(234L, TestUtils.getPropertyValue(connectionFactory, "channelCheckoutTimeout"));
		assertEquals(456,  TestUtils.getPropertyValue(connectionFactory, "connectionLimit"));
		assertSame(beanFactory.getBean(ConnectionNameStrategy.class),
				TestUtils.getPropertyValue(connectionFactory, "connectionNameStrategy"));
	}

	@Test
	public void testNative() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("native", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
	}

	@Test
	public void testWithExecutor() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("withExecutor", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		Object executor = new DirectFieldAccessor(connectionFactory).getPropertyValue("executorService");
		assertNotNull(executor);
		ThreadPoolTaskExecutor exec = beanFactory.getBean("exec", ThreadPoolTaskExecutor.class);
		assertSame(exec.getThreadPoolExecutor(), executor);
		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		assertEquals(Boolean.FALSE, dfa.getPropertyValue("publisherConfirms"));
		assertEquals(Boolean.FALSE, dfa.getPropertyValue("publisherReturns"));
		assertEquals(CachingConnectionFactory.CacheMode.CONNECTION, connectionFactory.getCacheMode());
		assertEquals(new ConnectionFactory().getConnectionTimeout(), // verify we didn't overwrite default
				TestUtils.getPropertyValue(connectionFactory, "rabbitConnectionFactory.connectionTimeout"));
		assertEquals(10, connectionFactory.getConnectionCacheSize());
	}

	@Test
	public void testWithExecutorService() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("withExecutorService", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
		Object executor = new DirectFieldAccessor(connectionFactory).getPropertyValue("executorService");
		assertNotNull(executor);
		ExecutorService exec = beanFactory.getBean("execService", ExecutorService.class);
		assertSame(exec, executor);
	}

	@Test
	public void testMultiHost() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("multiHost", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
		DirectFieldAccessor dfa =  new DirectFieldAccessor(connectionFactory);
		Address[] addresses = (Address[]) dfa.getPropertyValue("addresses");
		assertEquals(3, addresses.length);
		assertEquals("host1", addresses[0].getHost());
		assertEquals(1234, addresses[0].getPort());
		assertEquals("host2", addresses[1].getHost());
		assertEquals(-1, addresses[1].getPort());
		assertEquals("host3", addresses[2].getHost());
		assertEquals(4567, addresses[2].getPort());
		assertSame(beanFactory.getBean("tf"), TestUtils.getPropertyValue(connectionFactory,
				"rabbitConnectionFactory.threadFactory"));
	}

}
