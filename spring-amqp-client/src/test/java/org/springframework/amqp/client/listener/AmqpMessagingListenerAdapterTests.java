/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.listener;

import java.util.concurrent.CompletableFuture;

import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Tracker;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Artem Bilan
 *
 * @since 4.1.2
 */
class AmqpMessagingListenerAdapterTests {

	@Test
	void replySenderIsClosedAfterEachReply() throws Exception {
		DefaultMessageHandlerMethodFactory handlerMethodFactory = new DefaultMessageHandlerMethodFactory();
		handlerMethodFactory.afterPropertiesSet();
		HandlerAdapter handlerAdapter =
				new HandlerAdapter(handlerMethodFactory.createInvocableHandlerMethod(new TestListener(),
						TestListener.class.getMethod("handle", byte[].class)));
		AmqpMessagingListenerAdapter listenerAdapter = new AmqpMessagingListenerAdapter(handlerAdapter);

		Tracker tracker = mock();
		given(tracker.remoteState()).willReturn(DeliveryState.accepted());
		willReturn(CompletableFuture.completedFuture(tracker)).given(tracker).settlementFuture();

		Sender sender = mock();
		willReturn(CompletableFuture.completedFuture(sender)).given(sender).openFuture();
		willReturn(tracker).given(sender).send(any());

		Connection connection = mock();
		given(connection.openSender(anyString())).willReturn(sender);

		Receiver receiver = mock();
		given(receiver.connection()).willReturn(connection);

		Delivery delivery = mock();
		given(delivery.receiver()).willReturn(receiver);
		given(delivery.settled()).willReturn(true);
		willReturn(Message.create("test".getBytes()).replyTo("some_address"),
				Message.create("test".getBytes()).replyTo("some_address"))
				.given(delivery).message();

		listenerAdapter.onDelivery(delivery, null);
		listenerAdapter.onDelivery(delivery, null);

		verify(connection, times(2)).openSender("some_address");
		verify(sender, times(2)).close();
	}

	public static class TestListener {

		public String handle(byte[] payload) {
			return new String(payload).toUpperCase();
		}

	}

}
