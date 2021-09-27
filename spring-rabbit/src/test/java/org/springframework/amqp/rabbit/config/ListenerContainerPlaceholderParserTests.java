/*
 * Copyright 2010-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.ListenerContainerParserTests.TestBean;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Will Droste
 * @author Artem Bilan
 */
public final class ListenerContainerPlaceholderParserTests {

	private GenericApplicationContext context;

	@BeforeEach
	public void setUp() {
		this.context = new GenericXmlApplicationContext(
				new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@AfterEach
	public void closeBeanFactory() throws Exception {
		if (this.context != null) {
			CachingConnectionFactory cf = this.context.getBean(CachingConnectionFactory.class);
			this.context.close();
			ExecutorService es = TestUtils.getPropertyValue(cf, "channelsExecutor", ThreadPoolExecutor.class);
			if (es != null) {
				// if it gets started make sure its terminated..
				assertThat(es.isTerminated()).isTrue();
			}
		}
	}

	@Test
	public void testParseWithQueueNames() throws Exception {
		SimpleMessageListenerContainer container =
				this.context.getBean("testListener", SimpleMessageListenerContainer.class);
		assertThat(container.getAcknowledgeMode()).isEqualTo(AcknowledgeMode.MANUAL);
		assertThat(container.getConnectionFactory()).isEqualTo(this.context.getBean(ConnectionFactory.class));
		assertThat(container.getMessageListener().getClass()).isEqualTo(MessageListenerAdapter.class);
		DirectFieldAccessor listenerAccessor = new DirectFieldAccessor(container.getMessageListener());
		assertThat(listenerAccessor.getPropertyValue("delegate")).isEqualTo(this.context.getBean(TestBean.class));
		assertThat(listenerAccessor.getPropertyValue("defaultListenerMethod")).isEqualTo("handle");
		Queue queue = this.context.getBean("bar", Queue.class);
		assertThat(Arrays.asList(container.getQueueNames()).toString()).isEqualTo("[foo, " + queue.getName() + "]");
	}

	@Test
	public void commasInPropertyNames() {
		SimpleMessageListenerContainer container = this.context.getBean("commaProps1",
				SimpleMessageListenerContainer.class);
		assertThat(container.getQueueNames()).containsExactly("foo", "bar");
	}

	@Test
	public void commasInPropertyQueues() {
		SimpleMessageListenerContainer container = this.context.getBean("commaProps2",
				SimpleMessageListenerContainer.class);
		String[] queueNames = container.getQueueNames();
		assertThat(queueNames).hasSize(2);
		assertThat(queueNames[0]).isEqualTo("foo");
		assertThat(queueNames[1]).startsWith("spring.gen");
	}

}
