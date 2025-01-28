/*
 * Copyright 2021-2025 the original author or authors.
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Tx.SelectOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.2.18
 *
 */
public class MessageListenerContainerTxSynchTests {

	@Test
	void resourcesClearedAfterTxFails() throws IOException, TimeoutException, InterruptedException, ExecutionException {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.txSelect()).willReturn(mock(SelectOk.class));
		given(mockChannel.txCommit()).willThrow(IllegalStateException.class);
		AtomicReference<Consumer> consumer = new AtomicReference<>();
		AtomicReference<String> tag = new AtomicReference<>();
		CountDownLatch latch1 = new CountDownLatch(1);
		willAnswer(inv -> {
			consumer.set(inv.getArgument(6));
			consumer.get().handleConsumeOk(inv.getArgument(2));
			tag.set(inv.getArgument(2));
			latch1.countDown();
			return null;
		}).given(mockChannel).basicConsume(any(), anyBoolean(), any(), anyBoolean(), anyBoolean(), any(), any());
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.initialize();
		SimpleMessageListenerContainer mlc = new SimpleMessageListenerContainer();
		mlc.setConnectionFactory(connectionFactory);
		mlc.setQueueNames("foo");
		mlc.setTaskExecutor(exec);
		mlc.setChannelTransacted(true);
		mlc.setReceiveTimeout(10);
		CountDownLatch latch2 = new CountDownLatch(1);
		mlc.setMessageListener(msg -> {
			template.convertAndSend("foo", "bar");
			latch2.countDown();
		});
		mlc.start();
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		consumer.get().handleDelivery(tag.get(), new Envelope(1, false, "", ""), new AMQP.BasicProperties(),
				new byte[0]);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		mlc.stop();
		Future<Exception> exception = exec.submit(() -> {
			try {
				// verify no lingering resources bound to the executor's single thread
				assertThat(TransactionSynchronizationManager.hasResource(connectionFactory)).isFalse();
				assertThatIllegalStateException()
						.isThrownBy(() -> (TransactionSynchronizationManager.getSynchronizations()).isEmpty())
						.withMessage("Transaction synchronization is not active");
				return null;
			}
			catch (Exception e) {
				return e;
			}
		});
		assertThat(exception.get(10, TimeUnit.SECONDS)).isNull();
		exec.shutdown();
	}

}
