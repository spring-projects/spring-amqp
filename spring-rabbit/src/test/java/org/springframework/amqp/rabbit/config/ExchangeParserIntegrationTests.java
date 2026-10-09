/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public final class ExchangeParserIntegrationTests {

	@Autowired
	private ConnectionFactory connectionFactory;

	@Autowired
	private Exchange fanoutTest;

	@Autowired
	private Exchange directTest;

	@Autowired
	@Qualifier("bucket")
	private Queue queue;

	@Autowired
	@Qualifier("bucket2")
	private Queue queue2;

	@Autowired
	@Qualifier("bucket.test")
	private Queue queue3;

	@BeforeAll
	@AfterAll
	public static void clean() {
		RabbitAvailableCondition.getBrokerRunning().deleteExchanges("fanoutTest", "directTest", "topicTest",
				"headersTest", "headersTestMulti");
	}

	@Test
	public void testBindingsDeclared() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.convertAndSend(fanoutTest.getName(), "", "message");
		template.convertAndSend(fanoutTest.getName(), queue.getName(), "message");
		Thread.sleep(200);
		// The queue is anonymous so it will be deleted at the end of the test, but it should get the message as long as
		// we use the same connection
		String result = (String) template.receiveAndConvert(queue.getName());
		assertThat(result).isEqualTo("message");
		result = (String) template.receiveAndConvert(queue.getName());
		assertThat(result).isEqualTo("message");
	}

	@Test
	public void testDirectExchangeBindings() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		template.convertAndSend(directTest.getName(), queue.getName(), "message");
		Thread.sleep(200);

		String result = (String) template.receiveAndConvert(queue.getName());
		assertThat(result).isEqualTo("message");

		template.convertAndSend(directTest.getName(), "", "message2");
		Thread.sleep(200);

		assertThat(template.receiveAndConvert(queue.getName())).isNull();

		result = (String) template.receiveAndConvert(queue2.getName());
		assertThat(result).isEqualTo("message2");

		template.convertAndSend(directTest.getName(), queue2.getName(), "message2");
		Thread.sleep(200);

		assertThat(template.receiveAndConvert(queue2.getName())).isNull();

		template.convertAndSend(directTest.getName(), queue3.getName(), "message2");
		Thread.sleep(200);

		result = (String) template.receiveAndConvert(queue3.getName());
		assertThat(result).isEqualTo("message2");
	}

}
