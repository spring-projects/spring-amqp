/*
 * Copyright 2010-2014 the original author or authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.SerializerMessageConverter;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.support.RetryTemplate;

/**
 *
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public final class TemplateParserTests {

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testTemplate() throws Exception {
		AmqpTemplate template = beanFactory.getBean("template", AmqpTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		assertEquals(Boolean.FALSE, dfa.getPropertyValue("mandatory"));
		assertNull(dfa.getPropertyValue("returnCallback"));
		assertNull(dfa.getPropertyValue("confirmCallback"));
	}

	@Test
	public void testTemplateWithCallbacks() throws Exception {
		AmqpTemplate template = beanFactory.getBean("withCallbacks", AmqpTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		assertEquals(Boolean.TRUE, dfa.getPropertyValue("mandatory"));
		assertNotNull(dfa.getPropertyValue("returnCallback"));
		assertNotNull(dfa.getPropertyValue("confirmCallback"));
	}

	@Test
	public void testKitchenSink() throws Exception {
		RabbitTemplate template = beanFactory.getBean("kitchenSink", RabbitTemplate.class);
		assertNotNull(template);
		assertTrue(template.getMessageConverter() instanceof SerializerMessageConverter);
		DirectFieldAccessor accessor = new DirectFieldAccessor(template);
		assertEquals("foo", accessor.getPropertyValue("correlationKey"));
		assertSame(beanFactory.getBean(RetryTemplate.class), accessor.getPropertyValue("retryTemplate"));
	}

	@Test
	public void testWithReplyQ() throws Exception {
		RabbitTemplate template = beanFactory.getBean("withReplyQ", RabbitTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		assertNull(dfa.getPropertyValue("correlationKey"));
		Queue queue = (Queue) dfa.getPropertyValue("replyQueue");
		assertNotNull(queue);
		Queue queueBean = beanFactory.getBean("reply.queue", Queue.class);
		assertSame(queueBean, queue);
		SimpleMessageListenerContainer container = beanFactory.getBean("withReplyQ.replyListener", SimpleMessageListenerContainer.class);
		assertNotNull(container);
		dfa = new DirectFieldAccessor(container);
		assertSame(template, dfa.getPropertyValue("messageListener"));
		SimpleMessageListenerContainer messageListenerContainer = beanFactory.getBean(SimpleMessageListenerContainer.class);
		dfa = new DirectFieldAccessor(messageListenerContainer);
		Collection<?> queueNames = (Collection<?>) dfa.getPropertyValue("queueNames");
		assertEquals(1, queueNames.size());
		assertEquals(queueBean.getName(), queueNames.iterator().next());
	}

}
