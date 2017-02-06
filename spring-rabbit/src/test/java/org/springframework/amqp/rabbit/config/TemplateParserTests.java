/*
 * Copyright 2002-2017 the original author or authors.
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
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;

/**
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
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
		assertEquals(Boolean.FALSE, TestUtils.getPropertyValue(template, "mandatoryExpression.value"));
		assertNull(TestUtils.getPropertyValue(template, "returnCallback"));
		assertNull(TestUtils.getPropertyValue(template, "confirmCallback"));
		assertTrue(TestUtils.getPropertyValue(template, "useDirectReplyToContainer", Boolean.class));
	}

	@Test
	public void testTemplateWithCallbacks() throws Exception {
		AmqpTemplate template = beanFactory.getBean("withCallbacks", AmqpTemplate.class);
		assertNotNull(template);
		assertEquals("true", TestUtils.getPropertyValue(template, "mandatoryExpression.literalValue"));
		assertNotNull(TestUtils.getPropertyValue(template, "returnCallback"));
		assertNotNull(TestUtils.getPropertyValue(template, "confirmCallback"));
		assertFalse(TestUtils.getPropertyValue(template, "useDirectReplyToContainer", Boolean.class));
	}

	@Test
	public void testTemplateWithMandatoryExpression() throws Exception {
		AmqpTemplate template = beanFactory.getBean("withMandatoryExpression", AmqpTemplate.class);
		assertNotNull(template);
		assertEquals("'true'", TestUtils.getPropertyValue(template, "mandatoryExpression.expression"));
		assertEquals("'foo'",
				TestUtils.getPropertyValue(template, "sendConnectionFactorySelectorExpression.expression"));
		assertEquals("'foo'",
				TestUtils.getPropertyValue(template, "receiveConnectionFactorySelectorExpression.expression"));
		assertFalse(TestUtils.getPropertyValue(template, "useTemporaryReplyQueues", Boolean.class));
	}

	@Test
	public void testKitchenSink() throws Exception {
		RabbitTemplate template = beanFactory.getBean("kitchenSink", RabbitTemplate.class);
		assertNotNull(template);
		assertTrue(template.getMessageConverter() instanceof SerializerMessageConverter);
		DirectFieldAccessor accessor = new DirectFieldAccessor(template);
		assertEquals("foo", accessor.getPropertyValue("correlationKey"));
		assertSame(beanFactory.getBean(RetryTemplate.class), accessor.getPropertyValue("retryTemplate"));
		assertSame(beanFactory.getBean(RecoveryCallback.class), accessor.getPropertyValue("recoveryCallback"));
		assertEquals(123L, accessor.getPropertyValue("receiveTimeout"));
		assertEquals(1000L, accessor.getPropertyValue("replyTimeout"));
		assertEquals("foo", accessor.getPropertyValue("exchange"));
		assertEquals("bar", accessor.getPropertyValue("queue"));
		assertEquals("spam", accessor.getPropertyValue("routingKey"));
		assertTrue(TestUtils.getPropertyValue(template, "useTemporaryReplyQueues", Boolean.class));
		assertEquals("@connectionFactory.username",
				TestUtils.getPropertyValue(template, "userIdExpression.expression"));
	}

	@Test
	public void testWithReplyQ() throws Exception {
		RabbitTemplate template = beanFactory.getBean("withReplyQ", RabbitTemplate.class);
		assertNotNull(template);
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		assertNull(dfa.getPropertyValue("correlationKey"));
		String replyQueue = (String) dfa.getPropertyValue("replyAddress");
		assertNotNull(replyQueue);
		Queue queueBean = beanFactory.getBean("replyQId", Queue.class);
		assertEquals(queueBean.getName(), replyQueue);
		SimpleMessageListenerContainer container =
				beanFactory.getBean("withReplyQ.replyListener", SimpleMessageListenerContainer.class);
		assertNotNull(container);
		dfa = new DirectFieldAccessor(container);
		assertSame(template, dfa.getPropertyValue("messageListener"));
		SimpleMessageListenerContainer messageListenerContainer =
				beanFactory.getBean(SimpleMessageListenerContainer.class);
		dfa = new DirectFieldAccessor(messageListenerContainer);
		Collection<?> queueNames = (Collection<?>) dfa.getPropertyValue("queueNames");
		assertEquals(1, queueNames.size());
		assertEquals(queueBean.getName(), queueNames.iterator().next());
	}

}
