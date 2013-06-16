/*
 * Copyright 2002-2013 the original author or authors.
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

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 * @author Mark Fisher
 * @author Gary Russell
 * @since 1.0
 *
 */
public final class ExchangeParserTests {

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName()+"-context.xml", getClass()));
	}

	@Test
	public void testDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertFalse(exchange.shouldDeclare());
		assertEquals(2, exchange.getDeclaringAdmins().size());
		Binding binding = beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#0", Binding.class);
		assertFalse(binding.shouldDeclare());
		assertEquals(2, binding.getDeclaringAdmins().size());

	}

	@Test
	public void testAliasDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("alias", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-alias", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
	}

	@Test
	public void testTopicExchange() throws Exception {
		TopicExchange exchange = beanFactory.getBean("topic", TopicExchange.class);
		assertNotNull(exchange);
		assertEquals("topic", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());

	}

	@Test
	public void testFanoutExchange() throws Exception {
		FanoutExchange exchange = beanFactory.getBean("fanout", FanoutExchange.class);
		assertNotNull(exchange);
		assertEquals("fanout", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());

	}

	@Test
	public void testHeadersExchange() throws Exception {
		HeadersExchange exchange = beanFactory.getBean("headers", HeadersExchange.class);
		assertNotNull(exchange);
		assertEquals("headers", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());

	}

	@Test
	public void testDirectExchangeOverride() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-override", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-override", exchange.getName());
		assertFalse(exchange.isDurable());
		assertTrue(exchange.isAutoDelete());
	}

	@Test
	public void testDirectExchangeWithArguments() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-arguments", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-arguments", exchange.getName());
		assertEquals("bar", exchange.getArguments().get("foo"));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testFederatedDirectExchange() throws Exception {
		org.springframework.amqp.core.FederatedExchange exchange =
			beanFactory.getBean("fedDirect", org.springframework.amqp.core.FederatedExchange.class);
		assertNotNull(exchange);
		assertEquals("fedDirect", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertEquals("direct", exchange.getArguments().get("type"));
		assertEquals("upstream-set1", exchange.getArguments().get("upstream-set"));
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());
		Binding binding = beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#1", Binding.class);
		assertTrue(binding.shouldDeclare());
		assertEquals(1, binding.getDeclaringAdmins().size());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testFederatedTopicExchange() throws Exception {
		org.springframework.amqp.core.FederatedExchange exchange =
			beanFactory.getBean("fedTopic", org.springframework.amqp.core.FederatedExchange.class);
		assertNotNull(exchange);
		assertEquals("fedTopic", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertEquals("topic", exchange.getArguments().get("type"));
		assertEquals("upstream-set2", exchange.getArguments().get("upstream-set"));
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());
		Binding binding = beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#2", Binding.class);
		assertTrue(binding.shouldDeclare());
		assertEquals(1, binding.getDeclaringAdmins().size());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testFederatedFanoutExchange() throws Exception {
		org.springframework.amqp.core.FederatedExchange exchange =
			beanFactory.getBean("fedFanout", org.springframework.amqp.core.FederatedExchange.class);
		assertNotNull(exchange);
		assertEquals("fedFanout", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertEquals("fanout", exchange.getArguments().get("type"));
		assertEquals("upstream-set3", exchange.getArguments().get("upstream-set"));
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());
		Binding binding = beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#3", Binding.class);
		assertTrue(binding.shouldDeclare());
		assertEquals(1, binding.getDeclaringAdmins().size());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testFederatedHeadersExchange() throws Exception {
		org.springframework.amqp.core.FederatedExchange exchange =
			beanFactory.getBean("fedHeaders", org.springframework.amqp.core.FederatedExchange.class);
		assertNotNull(exchange);
		assertEquals("fedHeaders", exchange.getName());
		assertTrue(exchange.isDurable());
		assertFalse(exchange.isAutoDelete());
		assertEquals("headers", exchange.getArguments().get("type"));
		assertEquals("upstream-set4", exchange.getArguments().get("upstream-set"));
		assertTrue(exchange.shouldDeclare());
		assertEquals(1, exchange.getDeclaringAdmins().size());
		Binding binding = beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#4", Binding.class);
		assertTrue(binding.shouldDeclare());
		assertEquals(1, binding.getDeclaringAdmins().size());
	}

}
