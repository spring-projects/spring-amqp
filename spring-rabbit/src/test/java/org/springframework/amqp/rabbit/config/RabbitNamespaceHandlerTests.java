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

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Tomas Lukosius
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.0
 *
 */
public final class RabbitNamespaceHandlerTests {

	private DefaultListableBeanFactory beanFactory;

	@BeforeEach
	public void setUp() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testQueue() throws Exception {
		Queue queue = beanFactory.getBean("foo", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("foo");
	}

	@Test
	public void testAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("bar", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("bar");
	}

	@Test
	public void testAnonymousQueue() throws Exception {
		Queue queue = beanFactory.getBean("bucket", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isNotSameAs("bucket");
		assertThat(queue instanceof AnonymousQueue).isTrue();
	}

	@Test
	public void testExchanges() throws Exception {
		assertThat(beanFactory.getBean("direct-test", DirectExchange.class)).isNotNull();
		assertThat(beanFactory.getBean("topic-test", TopicExchange.class)).isNotNull();
		assertThat(beanFactory.getBean("fanout-test", FanoutExchange.class)).isNotNull();
		assertThat(beanFactory.getBean("headers-test", HeadersExchange.class)).isNotNull();
	}

	@Test
	public void testBindings() throws Exception {
		Map<String, Binding> bindings = beanFactory.getBeansOfType(Binding.class);
		// 4 for each exchange type
		assertThat(bindings).hasSize(13);
		for (Map.Entry<String, Binding> bindingEntry : bindings.entrySet()) {
			Binding binding = bindingEntry.getValue();
			if ("headers-test".equals(binding.getExchange()) && "bucket".equals(binding.getDestination())) {
				Map<String, Object> arguments = binding.getArguments();
				assertThat(arguments).hasSize(3);
				break;
			}
		}
	}

	@Test
	public void testAdmin() throws Exception {
		assertThat(beanFactory.getBean("admin-test", RabbitAdmin.class)).isNotNull();
	}

}
