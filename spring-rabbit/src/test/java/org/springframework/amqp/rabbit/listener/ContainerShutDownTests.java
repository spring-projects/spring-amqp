/*
 * Copyright 2017-present the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP.BasicProperties;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.util.StopWatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 2.0
 *
 */
@RabbitAvailable(queues = "test.shutdown")
public class ContainerShutDownTests {

	@Test
	public void testUninterruptibleListenerSMLC() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setReceiveTimeout(10);
		testUninterruptibleListener(container);
	}

	@Test
	public void testUninterruptibleListenerDMLC() throws Exception {
		DirectMessageListenerContainer container = new DirectMessageListenerContainer();
		testUninterruptibleListener(container);
	}

	public void testUninterruptibleListener(AbstractMessageListenerContainer container) throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		container.setConnectionFactory(cf);
		container.setQueueNames("test.shutdown");
		final CountDownLatch latch = new CountDownLatch(1);
		final CountDownLatch testEnded = new CountDownLatch(1);
		container.setMessageListener(m -> {
			try {
				latch.countDown();
				testEnded.await(30, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		final CountDownLatch startLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStartedEvent) {
				startLatch.countDown();
			}
		});
		Connection connection = cf.createConnection();
		Map<?, ?> channels = TestUtils.getPropertyValue(connection, "target.delegate._channelManager._channelMap",
				Map.class);
		container.start();
		try {
			assertThat(startLatch.await(30, TimeUnit.SECONDS)).isTrue();
			RabbitTemplate template = new RabbitTemplate(cf);
			template.execute(c -> {
				c.basicPublish("", "test.shutdown", new BasicProperties(), "foo".getBytes());
				RabbitUtils.setPhysicalCloseRequired(c, false);
				return null;
			});
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(channels).hasSize(2);
		}
		finally {
			testEnded.countDown();
			container.stop();
			cf.destroy();
		}
	}

	@Test
	public void consumersCorrectlyCancelledOnShutdownSMLC() throws Exception {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setReceiveTimeout(10);
		consumersCorrectlyCancelledOnShutdown(container);
	}

	@Test
	public void consumersCorrectlyCancelledOnShutdownDMLC() throws Exception {
		DirectMessageListenerContainer container = new DirectMessageListenerContainer();
		consumersCorrectlyCancelledOnShutdown(container);
	}

	private void consumersCorrectlyCancelledOnShutdown(AbstractMessageListenerContainer container)
			throws InterruptedException {

		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		container.setConnectionFactory(cf);
		container.setQueueNames("test.shutdown");
		container.setMessageListener(m -> {
		});
		final CountDownLatch startLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStartedEvent) {
				startLatch.countDown();
			}
		});
		container.start();
		try {
			assertThat(startLatch.await(30, TimeUnit.SECONDS)).isTrue();
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			container.shutdown();
			stopWatch.stop();
			assertThat(stopWatch.getTotalTimeMillis()).isLessThan(3000);
		}
		finally {
			cf.destroy();
		}
	}

	@Test
	void directMessageListenerContainerShutdownsItsSchedulerOnStopWithCallback() {
		DirectMessageListenerContainer container = new DirectMessageListenerContainer();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		container.setConnectionFactory(cf);
		container.setQueueNames("test.shutdown");
		container.setMessageListener(m -> {
		});

		container.start();

		ScheduledExecutorService scheduledExecutorService =
				TestUtils.getPropertyValue(container, "taskScheduler.scheduledExecutor", ScheduledExecutorService.class);

		container.stop(() -> {
		});

		cf.destroy();

		assertThat(scheduledExecutorService.isShutdown()).isTrue();
	}

}
