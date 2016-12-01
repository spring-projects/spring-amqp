/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.utils.test.TestUtils;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @since 1.0
 *
 */
public class BlockingQueueConsumerIntegrationTests {

	private static Queue queue1 = new Queue("test.queue1");

	private static Queue queue2 = new Queue("test.queue2");

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue1.getName(), queue2.getName());

	@Rule
	public LogLevelAdjuster logLevels = new LogLevelAdjuster(Level.INFO, RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			BlockingQueueConsumerIntegrationTests.class);

	@After
	public void tearDown() {
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testTransactionalLowLevel() throws Exception {

		RabbitTemplate template = new RabbitTemplate();
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, queue1.getName(), queue2.getName());
		final String consumerTagPrefix = UUID.randomUUID().toString();
		blockingQueueConsumer.setTagStrategy(queue -> consumerTagPrefix + '#' + queue);
		blockingQueueConsumer.start();
		assertNotNull(TestUtils.getPropertyValue(blockingQueueConsumer, "consumerTags", Map.class).get(
				consumerTagPrefix + "#" + queue1.getName()));
		assertNotNull(TestUtils.getPropertyValue(blockingQueueConsumer, "consumerTags", Map.class).get(
				consumerTagPrefix + "#" + queue2.getName()));

		// TODO: make this into a proper assertion. An exception can be thrown here by the Rabbit client and printed to
		// stderr without being rethrown (so hard to make a test fail).
		blockingQueueConsumer.stop();
		assertNull(template.receiveAndConvert(queue1.getName()));
		connectionFactory.destroy();

	}

	@Test
	public void testAvoidHangAMQP_508() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		String longName = new String(new byte[300]).replace('\u0000', 'x');
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, longName, "foobar");
		try {
			blockingQueueConsumer.start();
			fail("expected exception");
		}
		catch (FatalListenerStartupException e) {
			assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
		}
		connectionFactory.destroy();
	}

}
