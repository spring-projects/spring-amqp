/*
 * Copyright 2002-2015 the original author or authors.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.beans.DirectFieldAccessor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.0.1
 *
 */
public class BlockingQueueConsumerTests {

	@Test
	public void testRequeue() throws Exception {
		Exception ex = new RuntimeException();
		testRequeueOrNotDefaultYes(ex, true);
	}

	@Test
	public void testRequeueNullException() throws Exception {
		testRequeueOrNotDefaultYes(null, true);
	}

	@Test
	public void testDontRequeue() throws Exception {
		testRequeueOrNotDefaultYes(new AmqpRejectAndDontRequeueException("fail"), false);
	}

	@Test
	public void testDontRequeueNested() throws Exception {
		Exception ex = new RuntimeException(new RuntimeException(new AmqpRejectAndDontRequeueException("fail")));
		testRequeueOrNotDefaultYes(ex, false);
	}

	@Test
	public void testRequeueDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new RuntimeException(), false);
	}

	@Test
	public void testRequeueNullExceptionDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(null, false);
	}

	@Test
	public void testDontRequeueDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new AmqpRejectAndDontRequeueException("fail"), false);
	}

	@Test
	public void testDontRequeueNestedDefaultNot() throws Exception {
		Exception ex = new RuntimeException(new RuntimeException(new AmqpRejectAndDontRequeueException("fail")));
		testRequeueOrNotDefaultNo(ex, false);
	}

	/**
	 * We should always requeue if the exception is a
	 * {@link MessageRejectedWhileStoppingException}.
	 */
	@Test
	public void testDoRequeueStoppingDefaultNot() throws Exception {
		testRequeueOrNotDefaultNo(new MessageRejectedWhileStoppingException(), true);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPrefetchIsSetOnFailedPassiveDeclaration() throws IOException {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(Mockito.anyBoolean())).thenReturn(channel);
		when(channel.isOpen()).thenReturn(true);
		when(channel.queueDeclarePassive(Mockito.anyString()))
				.then(new Answer<Object>() {

					@Override
					public Object answer(InvocationOnMock invocation) throws Throwable {
						Object arg = invocation.getArguments()[0];
						if ("good".equals(arg)) {
							return Mockito.any(AMQP.Queue.DeclareOk.class);
						}
						else {
							throw new IOException();
						}
					}
				});
		when(channel.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
						anyMap(), any(Consumer.class))).thenReturn("consumerTag");

		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 20, "good", "bad");

		blockingQueueConsumer.setDeclarationRetries(1);
		blockingQueueConsumer.setRetryDeclarationInterval(10);
		blockingQueueConsumer.setFailedDeclarationRetryInterval(10);
		blockingQueueConsumer.start();

		verify(channel).basicQos(20);
	}

	private void testRequeueOrNotDefaultYes(Exception ex, boolean expectedRequeue) throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, "testQ");
		testRequeueOrNotGuts(ex, expectedRequeue, channel, blockingQueueConsumer);
	}

	private void testRequeueOrNotDefaultNo(Exception ex, boolean expectedRequeue) throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Channel channel = mock(Channel.class);
		BlockingQueueConsumer blockingQueueConsumer = new BlockingQueueConsumer(connectionFactory,
				new DefaultMessagePropertiesConverter(), new ActiveObjectCounter<BlockingQueueConsumer>(),
				AcknowledgeMode.AUTO, true, 1, false, "testQ");
		testRequeueOrNotGuts(ex, expectedRequeue, channel, blockingQueueConsumer);
	}

	private void testRequeueOrNotGuts(Exception ex, boolean expectedRequeue,
			Channel channel, BlockingQueueConsumer blockingQueueConsumer) throws Exception {
		DirectFieldAccessor dfa = new DirectFieldAccessor(blockingQueueConsumer);
		dfa.setPropertyValue("channel", channel);
		Set<Long> deliveryTags = new HashSet<Long>();
		deliveryTags.add(1L);
		dfa.setPropertyValue("deliveryTags", deliveryTags);
		blockingQueueConsumer.rollbackOnExceptionIfNecessary(ex);
		Mockito.verify(channel).basicReject(1L, expectedRequeue);
	}

}
