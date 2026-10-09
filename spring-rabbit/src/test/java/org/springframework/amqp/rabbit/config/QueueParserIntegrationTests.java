/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 *
 * @since 1.0
 *
 */
@RabbitAvailable
public final class QueueParserIntegrationTests {

	private DefaultListableBeanFactory beanFactory;

	@BeforeEach
	public void setUpDefaultBeanFactory() {
		beanFactory = new DefaultListableBeanFactory();
		XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
		reader.loadBeanDefinitions(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@Test
	public void testArgumentsQueue() {
		Queue queue = beanFactory.getBean("arguments", Queue.class);
		assertThat(queue).isNotNull();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.deleteQueue(queue.getName());
		rabbitAdmin.declareQueue(queue);

		assertThat(queue.getArguments().get("x-message-ttl")).isEqualTo(100L);
		template.convertAndSend(queue.getName(), "message");
		await().with().pollInterval(Duration.ofMillis(500))
				.until(() -> rabbitAdmin.getQueueProperties("arguments")
						.get(RabbitAdmin.QUEUE_MESSAGE_COUNT).equals(0L));
		connectionFactory.destroy();
		RabbitAvailableCondition.getBrokerRunning().deleteQueues("arguments");
	}

}
