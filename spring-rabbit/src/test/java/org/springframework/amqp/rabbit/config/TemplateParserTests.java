/*
 * Copyright 2010-2011 the original author or authors.
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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.SerializerMessageConverter;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * 
 * @author Dave Syer
 * @author Gary Russell
 * 
 */
public final class TemplateParserTests {

	private XmlBeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new XmlBeanFactory(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testTemplate() throws Exception {
		AmqpTemplate template = beanFactory.getBean("template", AmqpTemplate.class);
		assertNotNull(template);
	}	
	
	@Test
	public void testKitchenSink() throws Exception {
		RabbitTemplate template = beanFactory.getBean("kitchenSink", RabbitTemplate.class);
		assertNotNull(template);
		assertTrue(template.getMessageConverter() instanceof SerializerMessageConverter);
	}	

	@Test
	public void testWithArgs() throws Exception {
		RabbitTemplate template = beanFactory.getBean("withArgs", RabbitTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		Map<?, ?> args = (Map<?, ?>) dfa.getPropertyValue("replyQueueArguments");
		assertNotNull(args);
		assertEquals("bar", args.get("foo"));
	}

	@Test
	public void testWithAnonArgs() throws Exception {
		RabbitTemplate template = beanFactory.getBean("withAnonArgs", RabbitTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		Map<?, ?> args = (Map<?, ?>) dfa.getPropertyValue("replyQueueArguments");
		assertNotNull(args);
		assertEquals("qux", args.get("baz"));
	}
}
