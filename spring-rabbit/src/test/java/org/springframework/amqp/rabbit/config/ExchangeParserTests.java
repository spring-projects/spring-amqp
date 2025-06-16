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

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

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

	@BeforeEach
	public void setUp() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct", DirectExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("direct");
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.shouldDeclare()).isFalse();
		assertThat(exchange.getDeclaringAdmins()).hasSize(2);
		Binding binding =
				beanFactory.getBean("org.springframework.amqp.rabbit.config.BindingFactoryBean#0", Binding.class);
		assertThat(binding.shouldDeclare()).isFalse();
		assertThat(binding.getDeclaringAdmins()).hasSize(2);

		Map<String, Object> arguments = binding.getArguments();
		assertThat(arguments).isNotNull();
		assertThat(arguments).hasSize(1);
		assertThat(arguments.containsKey("x-match")).isTrue();
		assertThat(arguments.get("x-match")).isEqualTo("any");

	}

	@Test
	public void testAliasDirectExchange() throws Exception {
		DirectExchange exchange = beanFactory.getBean("alias", DirectExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("direct-alias");
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isAutoDelete()).isFalse();
	}

	@Test
	public void testTopicExchange() throws Exception {
		TopicExchange exchange = beanFactory.getBean("topic", TopicExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("topic");
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.shouldDeclare()).isTrue();
		assertThat(exchange.isDelayed()).isTrue();
		assertThat(exchange.isInternal()).isTrue();
		assertThat(exchange.getDeclaringAdmins()).hasSize(1);

	}

	@Test
	public void testFanoutExchange() throws Exception {
		FanoutExchange exchange = beanFactory.getBean("fanout", FanoutExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("fanout");
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.shouldDeclare()).isTrue();
		assertThat(exchange.isDelayed()).isFalse();
		assertThat(exchange.getDeclaringAdmins()).hasSize(1);

	}

	@Test
	public void testHeadersExchange() throws Exception {
		HeadersExchange exchange = beanFactory.getBean("headers", HeadersExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("headers");
		assertThat(exchange.isDurable()).isTrue();
		assertThat(exchange.isAutoDelete()).isFalse();
		assertThat(exchange.shouldDeclare()).isTrue();
		assertThat(exchange.getDeclaringAdmins()).hasSize(1);

	}

	@Test
	public void testDirectExchangeOverride() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-override", DirectExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("direct-override");
		assertThat(exchange.isDurable()).isFalse();
		assertThat(exchange.isAutoDelete()).isTrue();
	}

	@Test
	public void testDirectExchangeWithArguments() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-arguments", DirectExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("direct-arguments");
		assertThat(exchange.getArguments().get("foo")).isEqualTo("bar");
	}

	@Test
	public void testDirectExchangeWithReferencedArguments() throws Exception {
		DirectExchange exchange = beanFactory.getBean("direct-ref-arguments", DirectExchange.class);
		assertThat(exchange).isNotNull();
		assertThat(exchange.getName()).isEqualTo("direct-ref-arguments");
		assertThat(exchange.getArguments().get("foo")).isEqualTo("bar");
	}

}
