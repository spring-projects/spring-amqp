/*
 * Copyright 2002-2018 the original author or authors.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @since 1.0
 *
 */
public final class QueueParserIntegrationTests {

	@ClassRule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private DefaultListableBeanFactory beanFactory;

	@Before
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testArgumentsQueue() throws Exception {

		Queue queue = beanFactory.getBean("arguments", Queue.class);
		assertNotNull(queue);
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(
				brokerIsRunning.getConnectionFactory());
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.deleteQueue(queue.getName());
		rabbitAdmin.declareQueue(queue);

		assertEquals(100L, queue.getArguments().get("x-message-ttl"));
		template.convertAndSend(queue.getName(), "message");
		Properties props = rabbitAdmin.getQueueProperties("arguments");
		if (props != null) {
			int n = 0;
			while (n++ < 200 && (Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT) > 0) {
				Thread.sleep(50);
				props = rabbitAdmin.getQueueProperties("arguments");
			}
			assertThat((Integer) props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT), equalTo(0));
		}

		connectionFactory.destroy();
		brokerIsRunning.deleteQueues("arguments");
	}

}
