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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

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
 * @author Felipe Gutierrez
 * @author Artem Bilan
 * @since 1.0
 *
 */
public final class ExchangeParserTests {

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUp() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
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
		Binding binding =
				beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#0", Binding.class);
		assertFalse(binding.shouldDeclare());
		assertEquals(2, binding.getDeclaringAdmins().size());

		Map<String, Object> arguments = binding.getArguments();
		assertNotNull(arguments);
		assertEquals(1, arguments.size());
		assertTrue(arguments.containsKey("x-match"));
		assertEquals("any", arguments.get("x-match"));

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
		assertTrue(exchange.isDelayed());
		assertTrue(exchange.isInternal());
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
		assertFalse(exchange.isDelayed());
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

	@Test
	public void testDirectExchangeWithReferencedArguments() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-ref-arguments", DirectExchange.class);
		assertNotNull(exchange);
		assertEquals("direct-ref-arguments", exchange.getName());
		assertEquals("bar", exchange.getArguments().get("foo"));
	}

}
