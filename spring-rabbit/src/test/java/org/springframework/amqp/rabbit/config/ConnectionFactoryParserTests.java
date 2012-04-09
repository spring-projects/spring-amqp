/*
 * Copyright 2010-2012 the original author or authors.
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
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

import com.rabbitmq.client.Address;

/**
 * 
 * @author Dave Syer
 * @author Gary Russell
 * 
 */
public final class ConnectionFactoryParserTests {

	private XmlBeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testKitchenSink() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("kitchenSink", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
	}	
	
	@Test
	public void testNative() throws Exception {
		CachingConnectionFactory connectionFactory = beanFactory.getBean("native", CachingConnectionFactory.class);
		assertNotNull(connectionFactory);
		assertEquals(10, connectionFactory.getChannelCacheSize());
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
	}

}
