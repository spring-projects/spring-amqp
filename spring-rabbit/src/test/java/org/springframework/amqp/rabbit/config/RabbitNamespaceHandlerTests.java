/*
 * Copyright 2002-present the original author or authors.
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
