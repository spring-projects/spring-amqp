/*
 * Copyright 2019-present the original author or authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel.Listener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.1.5
 *
 */
@RabbitAvailable
public class PublisherCallbackChannelTests {

	@Test
	void correlationData() {
		CorrelationData cd = new CorrelationData();
		assertThat(cd.getReturned()).isNull();
	}

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
	@LogLevels(classes = { CachingConnectionFactory.class, AbstractConnectionFactory.class,
			PublisherCallbackChannelImpl.class })
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
		await().until(() -> {
			Properties props = cf.getCacheProperties();
			return Integer.parseInt(props.getProperty("idleChannelsNotTx")) == 2;
		});
		closeLatch.countDown();
		assertThat(confirmLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(closedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		await().until(() -> {
			Properties props = cf.getCacheProperties();
			return Integer.parseInt(props.getProperty("idleChannelsNotTx")) == 2;
		});
	}

	@Test
	void confirmAlwaysAfterReturn() throws InterruptedException {
		Channel delegate = mock(Channel.class);
		ExecutorService executor = Executors.newCachedThreadPool();
		PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(delegate, executor);
		TheListener listener = new TheListener();
		channel.addListener(listener);
		channel.addPendingConfirm(listener, 1, new PendingConfirm(new CorrelationData("1"), 0L));
		HashMap<String, Object> headers = new HashMap<>();
		LongString correlation = mock(LongString.class);
		given(correlation.getBytes()).willReturn("1".getBytes());
		given(correlation.toString()).willReturn("1");
		headers.put(PublisherCallbackChannelImpl.RETURNED_MESSAGE_CORRELATION_KEY, correlation);
		headers.put(PublisherCallbackChannelImpl.RETURN_LISTENER_CORRELATION_KEY, listener.getUUID());
		BasicProperties properties = new BasicProperties.Builder().headers(headers).build();
		channel.handle(new Return(0, "", "", "", properties, new byte[0]));
		channel.handleAck(1, false);
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		channel.addPendingConfirm(listener, 2, new PendingConfirm(new CorrelationData("1"), 0L));
		channel.handle(new Return(0, "", "", "", properties, new byte[0]));
		channel.handleAck(2, false);
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.calls).containsExactly("return", "confirm", "return", "confirm");
	}

	private static final class TheListener implements Listener {

		private final UUID uuid = UUID.randomUUID();

		final List<String> calls = Collections.synchronizedList(new ArrayList<>());

		final CountDownLatch latch1 = new CountDownLatch(2);

		final CountDownLatch latch2 = new CountDownLatch(4);

		@Override
		public void handleConfirm(PendingConfirm pendingConfirm, boolean ack) {
			this.calls.add("confirm");
			this.latch1.countDown();
			this.latch2.countDown();
		}

		@Override
		public void handleReturn(Return returned) {
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			this.calls.add("return");
			this.latch1.countDown();
			this.latch2.countDown();
		}

		@Override
		public void revoke(Channel channel) {
		}

		@Override
		public String getUUID() {
			return this.uuid.toString();
		}

		@Override
		public boolean isConfirmListener() {
			return true;
		}

		@Override
		public boolean isReturnListener() {
			return true;
		}

	}

}
