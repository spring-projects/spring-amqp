/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Gary Russell
 * @since 2.1.5
 *
 */
@RabbitAvailable
public class PublisherCallbackChannelTests {

	@Test
	void shutdownWhileCreate() throws IOException, TimeoutException {
		Channel delegate = mock(Channel.class);
		AtomicBoolean npe = new AtomicBoolean();
		willAnswer(inv -> {
			ShutdownListener sdl = inv.getArgument(0);
			try {
				sdl.shutdownCompleted(new ShutdownSignalException(true, false, mock(Method.class), null));
			}
			catch (@SuppressWarnings("unused") NullPointerException e) {
				npe.set(true);
			}
			return null;
		}).given(delegate).addShutdownListener(any());
		PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(delegate, mock(ExecutorService.class));
		assertThat(npe.get()).isFalse();
		channel.close();
	}

	@Test
	void testNotCached() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		cf.setPublisherConfirmType(ConfirmType.CORRELATED);
		cf.setChannelCacheSize(2);
		cf.afterPropertiesSet();
		RabbitTemplate template = new RabbitTemplate(cf);
		CountDownLatch confirmLatch = new CountDownLatch(2);
		template.setConfirmCallback((correlationData, ack, cause) -> {
			try {
				Thread.sleep(50);
				confirmLatch.countDown();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		CountDownLatch openedLatch = new CountDownLatch(2);
		CountDownLatch closeLatch = new CountDownLatch(1);
		CountDownLatch closedLatch = new CountDownLatch(2);
		CountDownLatch waitForOtherLatch = new CountDownLatch(1);
		SimpleAsyncTaskExecutor exec = new SimpleAsyncTaskExecutor();
		IntStream.range(0, 2).forEach(i -> {
			// this will open 3 or 4 channels
			exec.execute(() -> {
				template.execute(chann -> {
					openedLatch.countDown();
					template.convertAndSend("", "foo", "msg", msg -> {
						if (i == 0) {
							try {
								waitForOtherLatch.await();
							}
							catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
						}
						else {
							waitForOtherLatch.countDown();
						}
						return msg;
					}, new CorrelationData("" + i));
					closeLatch.await();
					return null;
				});
				closedLatch.countDown();
			});
		});
		assertThat(openedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		Connection conn = cf.createConnection();
		Channel chan1 = conn.createChannel(false);
		Channel chan2 = conn.createChannel(false);
		chan1.close();
		chan2.close();
		Properties cacheProperties = cf.getCacheProperties();
		assertThat(cacheProperties.getProperty("idleChannelsNotTx")).isEqualTo("2");
		closeLatch.countDown();
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(closedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		cacheProperties = cf.getCacheProperties();
		await().until(() -> {
			Properties props = cf.getCacheProperties();
			return Integer.parseInt(props.getProperty("idleChannelsNotTx")) == 2;
		});
	}

}
