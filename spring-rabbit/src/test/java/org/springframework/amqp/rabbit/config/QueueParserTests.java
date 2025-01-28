/*
 * Copyright 2002-2025 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Felipe Gutierrez
 * @since 1.0
 *
 */
public class QueueParserTests {

	protected BeanFactory beanFactory;

	@BeforeEach
	public void setUpDefaultBeanFactory() throws Exception {
		DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
		this.beanFactory = beanFactory;
	}

	@Test
	public void testQueue() throws Exception {
		Queue queue = beanFactory.getBean("foo", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("foo");
		assertThat(queue.isDurable()).isTrue();
		assertThat(queue.isAutoDelete()).isFalse();
		assertThat(queue.isExclusive()).isFalse();
	}

	@Test
	public void testAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("alias", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("spam");
		assertThat(queue.getName()).isNotSameAs("alias");
	}

	@Test
	public void testOverrideQueue() throws Exception {
		Queue queue = beanFactory.getBean("override", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("override");
		assertThat(queue.isDurable()).isTrue();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isAutoDelete()).isTrue();
	}

	@Test
	public void testOverrideAliasQueue() throws Exception {
		Queue queue = beanFactory.getBean("overrideAlias", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isEqualTo("bar");
		assertThat(queue.isDurable()).isTrue();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isAutoDelete()).isTrue();
	}

	@Test
	public void testAnonymousQueue() throws Exception {
		Queue queue = beanFactory.getBean("anonymous", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isNotSameAs("anonymous");
		assertThat(queue instanceof AnonymousQueue).isTrue();
		assertThat(queue.isDurable()).isFalse();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isAutoDelete()).isTrue();
		assertThat(queue.getName()).matches("spring.gen-[0-9A-Za-z_\\-]{22}");
	}

	@Test
	public void testAnonymousQueueSpringName() throws Exception {
		Queue queue = beanFactory.getBean("uuidAnon", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isNotSameAs("anonymous");
		assertThat(queue instanceof AnonymousQueue).isTrue();
		assertThat(queue.isDurable()).isFalse();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isAutoDelete()).isTrue();
		assertThat(queue.getName()).matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}");
	}

	@Test
	public void testAnonymousQueueCustomName() throws Exception {
		Queue queue = beanFactory.getBean("customAnon", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getName()).isNotSameAs("anonymous");
		assertThat(queue instanceof AnonymousQueue).isTrue();
		assertThat(queue.isDurable()).isFalse();
		assertThat(queue.isExclusive()).isTrue();
		assertThat(queue.isAutoDelete()).isTrue();
		assertThat(queue.getName()).matches("custom.gen-[0-9A-Za-z_\\-]{22}");
	}

	@Test
	public void testReferenceArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("refArguments", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getArguments().get("foo")).isEqualTo("bar");
		assertThat(queue.getArguments().get("x-message-ttl")).isEqualTo(200L);
		assertThat(queue.getArguments().get("x-ha-policy")).isEqualTo("all");
	}

	@Test
	public void testArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("arguments", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getArguments().get("foo")).isEqualTo("bar");
		assertThat(queue.getArguments().get("x-message-ttl")).isEqualTo(100L);
		assertThat(queue.getArguments().get("x-ha-policy")).isEqualTo("all");
	}

	@Test
	public void testAnonymousArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("anonymousArguments", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getArguments().get("foo")).isEqualTo("spam");
	}

	@Test
	public void testReferencedArgumentsQueue() throws Exception {
		Queue queue = beanFactory.getBean("referencedArguments", Queue.class);
		assertThat(queue).isNotNull();
		assertThat(queue.getArguments().get("baz")).isEqualTo("qux");
	}

	@Test
	public void testDeclaredBy() throws Exception {
		Queue queue = beanFactory.getBean("autoDeclareTwoAdmins", Queue.class);
		RabbitAdmin admin1 = beanFactory.getBean("admin1", RabbitAdmin.class);
		RabbitAdmin admin2 = beanFactory.getBean("admin2", RabbitAdmin.class);
		assertThat(queue.getDeclaringAdmins()).hasSize(2);
		assertThat(queue.getDeclaringAdmins().contains(admin1)).isTrue();
		assertThat(queue.getDeclaringAdmins().contains(admin2)).isTrue();
		assertThat(queue.shouldDeclare()).isTrue();

		queue = beanFactory.getBean("autoDeclareOneAdmin", Queue.class);
		assertThat(queue.getDeclaringAdmins()).hasSize(1);
		assertThat(queue.getDeclaringAdmins().contains(admin1)).isTrue();
		assertThat(queue.getDeclaringAdmins().contains(admin2)).isFalse();
		assertThat(queue.shouldDeclare()).isTrue();

		queue = beanFactory.getBean("noAutoDeclare", Queue.class);
		assertThat(queue.getDeclaringAdmins()).hasSize(0);
		assertThat(queue.shouldDeclare()).isFalse();
	}

	@Test
	public void testIllegalAnonymousQueue() {
		DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(bf);
		assertThatExceptionOfType(BeanDefinitionStoreException.class).isThrownBy(() ->
			reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName()
				+ "IllegalAnonymous-context.xml", getClass())));
	}

}
