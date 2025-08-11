/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate.RecoveryCallback;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.SerializerMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.retry.RetryTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public final class TemplateParserTests {

	private DefaultListableBeanFactory beanFactory;

	@BeforeEach
	public void setUpDefaultBeanFactory() {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testTemplate() {
		AmqpTemplate template = beanFactory.getBean("template", AmqpTemplate.class);
		assertThat(template).isNotNull();
		assertThat(TestUtils.getPropertyValue(template, "mandatoryExpression.value")).isEqualTo(Boolean.FALSE);
		assertThat(TestUtils.getPropertyValue(template, "returnsCallback")).isNull();
		assertThat(TestUtils.getPropertyValue(template, "confirmCallback")).isNull();
		assertThat(TestUtils.getPropertyValue(template, "useDirectReplyToContainer", Boolean.class)).isTrue();
	}

	@Test
	public void testTemplateWithCallbacks() {
		AmqpTemplate template = beanFactory.getBean("withCallbacks", AmqpTemplate.class);
		assertThat(template).isNotNull();
		assertThat(TestUtils.getPropertyValue(template, "mandatoryExpression.literalValue")).isEqualTo("true");
		assertThat(TestUtils.getPropertyValue(template, "returnsCallback")).isNotNull();
		assertThat(TestUtils.getPropertyValue(template, "confirmCallback")).isNotNull();
		assertThat(TestUtils.getPropertyValue(template, "useDirectReplyToContainer", Boolean.class)).isFalse();
	}

	@Test
	public void testTemplateWithMandatoryExpression() {
		AmqpTemplate template = beanFactory.getBean("withMandatoryExpression", AmqpTemplate.class);
		assertThat(template).isNotNull();
		assertThat(TestUtils.getPropertyValue(template, "mandatoryExpression.expression")).isEqualTo("'true'");
		assertThat(TestUtils.getPropertyValue(template, "sendConnectionFactorySelectorExpression.expression"))
				.isEqualTo("'foo'");
		assertThat(TestUtils.getPropertyValue(template, "receiveConnectionFactorySelectorExpression.expression"))
				.isEqualTo("'foo'");
		assertThat(TestUtils.getPropertyValue(template, "useTemporaryReplyQueues", Boolean.class)).isFalse();
	}

	@Test
	public void testKitchenSink() {
		RabbitTemplate template = beanFactory.getBean("kitchenSink", RabbitTemplate.class);
		assertThat(template).isNotNull();
		assertThat(template.getMessageConverter() instanceof SerializerMessageConverter).isTrue();
		DirectFieldAccessor accessor = new DirectFieldAccessor(template);
		assertThat(accessor.getPropertyValue("correlationKey")).isEqualTo("foo");
		assertThat(accessor.getPropertyValue("retryTemplate")).isSameAs(beanFactory.getBean(RetryTemplate.class));
		assertThat(accessor.getPropertyValue("recoveryCallback")).isSameAs(beanFactory.getBean(RecoveryCallback.class));
		assertThat(accessor.getPropertyValue("receiveTimeout")).isEqualTo(123L);
		assertThat(accessor.getPropertyValue("replyTimeout")).isEqualTo(1000L);
		assertThat(accessor.getPropertyValue("exchange")).isEqualTo("foo");
		assertThat(accessor.getPropertyValue("defaultReceiveQueue")).isEqualTo("bar");
		assertThat(accessor.getPropertyValue("routingKey")).isEqualTo("spam");
		assertThat(TestUtils.getPropertyValue(template, "useTemporaryReplyQueues", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(template, "userIdExpression.expression"))
				.isEqualTo("@connectionFactory.username");
	}

	@Test
	public void testWithReplyQ() {
		RabbitTemplate template = beanFactory.getBean("withReplyQ", RabbitTemplate.class);
		assertThat(template).isNotNull();
		DirectFieldAccessor dfa = new DirectFieldAccessor(template);
		assertThat(dfa.getPropertyValue("correlationKey")).isNull();
		String replyQueue = (String) dfa.getPropertyValue("replyAddress");
		assertThat(replyQueue).isNotNull();
		Queue queueBean = beanFactory.getBean("replyQId", Queue.class);
		assertThat(replyQueue).isEqualTo(queueBean.getName());
		SimpleMessageListenerContainer container =
				beanFactory.getBean("withReplyQ.replyListener", SimpleMessageListenerContainer.class);
		assertThat(container).isNotNull();
		dfa = new DirectFieldAccessor(container);
		assertThat(dfa.getPropertyValue("messageListener")).isSameAs(template);
		SimpleMessageListenerContainer messageListenerContainer =
				beanFactory.getBean(SimpleMessageListenerContainer.class);
		dfa = new DirectFieldAccessor(messageListenerContainer);
		Collection<?> queueNames = (Collection<?>) dfa.getPropertyValue("queues");
		assertThat(queueNames).hasSize(1);
		assertThat(queueNames.iterator().next()).isEqualTo(queueBean);
	}

}
