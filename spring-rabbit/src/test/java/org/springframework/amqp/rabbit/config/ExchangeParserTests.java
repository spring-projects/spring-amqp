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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

public final class ExchangeParserTests {

	private XmlBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName()+"-context.xml", getClass()));
	}

	@Test
	public void testDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct", exchange.getName());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testAliasDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("alias", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-alias", exchange.getName());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testTopicExchange() throws Exception {
		TopicExchange exchange = beanFactory.getBean("topic", TopicExchange.class);
		assertNotNull(exchange);
		assertEquals("topic", exchange.getName());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testFanoutExchange() throws Exception {
		FanoutExchange exchange = beanFactory.getBean("fanout", FanoutExchange.class);
		assertNotNull(exchange);
		assertEquals("fanout", exchange.getName());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testHeadersExchange() throws Exception {
		HeadersExchange exchange = beanFactory.getBean("headers", HeadersExchange.class);
		assertNotNull(exchange);
		assertEquals("headers", exchange.getName());
		assertFalse(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testDirectExchangeOverride() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-override", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-override", exchange.getName());
		assertTrue(exchange.isDurable());
		assertTrue(exchange.isAutoDelete());
	}

	@Test
	public void testDirectExchangeWithArguments() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-arguments", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-arguments", exchange.getName());
		assertEquals("bar", exchange.getArguments().get("foo"));
	}

}
