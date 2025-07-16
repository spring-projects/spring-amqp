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

package org.springframework.amqp.rabbit.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.support.ActiveObjectCounter;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.0
 *
 */
@RabbitAvailable(queues = { BlockingQueueConsumerIntegrationTests.QUEUE1_NAME,
		BlockingQueueConsumerIntegrationTests.QUEUE2_NAME })
@LogLevels(classes = {RabbitTemplate.class,
			SimpleMessageListenerContainer.class, BlockingQueueConsumer.class,
			BlockingQueueConsumerIntegrationTests.class }, level = "INFO")
public class BlockingQueueConsumerIntegrationTests {

	public static final String QUEUE1_NAME = "test.queue1.BlockingQueueConsumerIntegrationTests";

	public static final String QUEUE2_NAME = "test.queue2.BlockingQueueConsumerIntegrationTests";

	private static Queue queue1 = new Queue(QUEUE1_NAME);

	private static Queue queue2 = new Queue(QUEUE2_NAME);

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
		CountDownLatch latch = new CountDownLatch(2);
		List<ConsumeOkEvent> events = new ArrayList<>();
		blockingQueueConsumer.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumeOkEvent consumeOkEvent) {
				events.add(consumeOkEvent);
				latch.countDown();
			}
		});
		blockingQueueConsumer.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(events.get(0).getConsumerTag()).isEqualTo(consumerTagPrefix + "#" + queue1.getName());
		assertThat(events.get(1).getConsumerTag()).isEqualTo(consumerTagPrefix + "#" + queue2.getName());
		blockingQueueConsumer.stop();
		assertThat(template.receiveAndConvert(queue1.getName())).isNull();
		connectionFactory.destroy();
	}

	@Test
	public void testAvoidHangAMQP_508() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		String longName = new String(new byte[300]).replace('\u0000', 'x');
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, longName, "foobar");
		assertThatExceptionOfType(FatalListenerStartupException.class)
				.isThrownBy(blockingQueueConsumer::start)
				.withCauseInstanceOf(IllegalArgumentException.class);
		connectionFactory.destroy();
	}

}
