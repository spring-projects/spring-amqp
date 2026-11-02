/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.listener;

import java.util.concurrent.CompletableFuture;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.config.MethodAmqpListenerEndpoint;
import org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.ReflectionUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Artem Bilan
 * @author WooSeung Kim
 *
 * @since 4.1.3
 */
class AmqpMessagingListenerAdapterTests {

	final AmqpAcknowledgment acknowledgment = mock();

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

		Session session = mock();
		given(session.openSender(anyString())).willReturn(sender);

		Receiver receiver = mock();
		given(receiver.session()).willReturn(session);

		Delivery delivery = mock();
		given(delivery.receiver()).willReturn(receiver);
		given(delivery.settled()).willReturn(true);
		willReturn(Message.create("test".getBytes()).replyTo("some_address"),
				Message.create("test".getBytes()).replyTo("some_address"))
				.given(delivery).message();

		listenerAdapter.onDelivery(delivery, null);
		listenerAdapter.onDelivery(delivery, null);

		verify(session, times(2)).openSender("some_address");
		verify(sender, times(2)).close();
	}

	@Test
	void completableFutureReplyFailureRejectsDelivery() throws Exception {
		verifyReplyFailureRejectsDelivery("completableFutureReply");
	}

	@Test
	void monoReplyFailureRejectsDelivery() throws Exception {
		verifyReplyFailureRejectsDelivery("monoReply");
	}

	@Test
	void monoReplyFailureHandledByErrorHandlerAcceptsDeliveryOnce() throws Exception {
		AmqpMessagingListenerAdapter adapter = createAdapter("monoReply");
		adapter.setErrorHandler(mock());

		adapter.onDelivery(delivery(), this.acknowledgment);

		verify(this.acknowledgment).acknowledge(AmqpAcknowledgment.Status.ACCEPT);
		verifyNoMoreInteractions(this.acknowledgment);
	}

	@Test
	void emptyMonoAcceptsDelivery() throws Exception {
		AmqpMessagingListenerAdapter adapter = createAdapter("emptyMono");

		adapter.onDelivery(delivery(), this.acknowledgment);

		verify(this.acknowledgment).acknowledge(AmqpAcknowledgment.Status.ACCEPT);
		verifyNoMoreInteractions(this.acknowledgment);
	}

	private void verifyReplyFailureRejectsDelivery(String methodName) throws Exception {
		AmqpMessagingListenerAdapter adapter = createAdapter(methodName);

		adapter.onDelivery(delivery(), this.acknowledgment);

		// The endpoint does not requeue rejected messages by default
		verify(this.acknowledgment).acknowledge(AmqpAcknowledgment.Status.REJECT);
		verifyNoMoreInteractions(this.acknowledgment);
	}

	private static AmqpMessagingListenerAdapter createAdapter(String methodName) {
		MethodAmqpListenerEndpoint endpoint =
				new MethodAmqpListenerEndpoint(new ReplyingBean(),
						ReflectionUtils.findMethod(ReplyingBean.class, methodName, String.class), "test.queue");

		AmqpConnectionFactory connectionFactory = mock();
		MethodAmqpMessageListenerContainerFactory factory = new MethodAmqpMessageListenerContainerFactory(connectionFactory);
		factory.setBeanFactory(new StaticListableBeanFactory());
		AmqpMessageListenerContainer container = factory.createContainer(endpoint);
		AmqpMessagingListenerAdapter adapter = (AmqpMessagingListenerAdapter) container.getMessageListener();
		adapter.containerAckMode(AcknowledgeMode.MANUAL);
		return adapter;
	}

	private static Delivery delivery() throws Exception {
		Delivery delivery = mock();
		// No reply-to and no default response address, so the reply cannot be sent
		given(delivery.<byte[]>message()).willReturn(Message.create("test data".getBytes()));
		return delivery;
	}

	public static class TestListener {

		public String handle(byte[] payload) {
			return new String(payload).toUpperCase();
		}

	}

	static class ReplyingBean {

		CompletableFuture<String> completableFutureReply(String data) {
			return CompletableFuture.completedFuture(data.toUpperCase());
		}

		Mono<String> monoReply(String data) {
			return Mono.just(data.toUpperCase());
		}

		Mono<String> emptyMono(String data) {
			return Mono.empty();
		}

	}

}
