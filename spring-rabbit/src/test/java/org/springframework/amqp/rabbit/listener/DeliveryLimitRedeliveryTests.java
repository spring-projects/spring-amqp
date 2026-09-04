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

package org.springframework.amqp.rabbit.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that a failing listener causes the broker to bump {@code x-delivery-count}, so a
 * quorum queue {@code x-delivery-limit} is eventually reached and the message is
 * dead-lettered instead of being redelivered forever.
 * <p>
 * RabbitMQ only counts deliveries rejected individually; it does not count those settled
 * by a {@code basic.nack} with {@code multiple=true}, which is what both containers used
 * to issue for a listener that is not returning an async reply.
 *
 * @author Artem Bilan
 *
 * @since 4.2
 *
 * @see <a href="https://github.com/spring-projects/spring-amqp/issues/3507">GH-3507</a>
 */
@RabbitAvailable
public class DeliveryLimitRedeliveryTests {

	private static final int DELIVERY_LIMIT = 3;

	private static final String QUEUE = "test.delivery.limit";

	private static final String DLQ = QUEUE + ".dlq";

	private final CachingConnectionFactory connectionFactory =
			new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());

	private final RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);

	@BeforeEach
	void declareQueues() {
		// A quorum queue cannot be auto-delete or exclusive, hence the explicit clean up.
		this.admin.deleteQueue(QUEUE);
		this.admin.deleteQueue(DLQ);
		this.admin.declareQueue(QueueBuilder.durable(QUEUE)
				.quorum()
				.deliveryLimit(DELIVERY_LIMIT)
				.deadLetterExchange("")
				.deadLetterRoutingKey(DLQ)
				.build());
		this.admin.declareQueue(QueueBuilder.durable(DLQ).build());
	}

	@AfterEach
	void deleteQueues() {
		this.admin.deleteQueue(QUEUE);
		this.admin.deleteQueue(DLQ);
		this.connectionFactory.destroy();
	}

	@Test
	void simpleContainerRejectionIsCountedTowardsDeliveryLimit() throws InterruptedException {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
		container.setReceiveTimeout(10);
		verifyDeliveryLimitIsReached(container);
	}

	@Test
	void directContainerRejectionIsCountedTowardsDeliveryLimit() throws InterruptedException {
		verifyDeliveryLimitIsReached(new DirectMessageListenerContainer(this.connectionFactory));
	}

	/**
	 * A listener may settle the delivery itself and still fail afterwards. The container must
	 * not reject it again: the broker answers a re-settle with
	 * {@code PRECONDITION_FAILED - unknown delivery tag} and closes the whole channel.
	 * {@code ConsumerBatchingTests.rejectOneReplayRest()} covers the same for the
	 * {@code SimpleMessageListenerContainer}.
	 */
	@Test
	void directContainerKeepsChannelWhenListenerSettledTheDeliveryItself() throws InterruptedException {
		AtomicBoolean first = new AtomicBoolean(true);
		// The consumer's channel for each invocation; the container recreates the consumer
		// (with a fresh channel) if the broker closes the current one.
		List<Channel> channels = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch secondLatch = new CountDownLatch(1);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(QUEUE);
		container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
			channels.add(((ChannelProxy) channel).getTargetChannel());
			if (first.getAndSet(false)) {
				channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
				throw new RuntimeException("intentional: settled by the listener, then failed");
			}
			secondLatch.countDown();
		});
		container.afterPropertiesSet();
		container.start();

		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		try {
			template.convertAndSend(QUEUE, "foo");
			// The listener rejected it without requeue, hence it is dead-lettered right away.
			assertThat(template.receive(DLQ, 30_000)).isNotNull();

			template.convertAndSend(QUEUE, "bar");
			assertThat(secondLatch.await(30, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}

		assertThat(channels).hasSize(2);
		assertThat(channels.get(1))
				.withFailMessage("the channel was closed by re-settling a delivery the listener had settled itself")
				.isSameAs(channels.get(0));
	}

	private void verifyDeliveryLimitIsReached(AbstractMessageListenerContainer container) throws InterruptedException {
		AtomicInteger deliveries = new AtomicInteger();
		// The broker dead-letters once 'x-delivery-count' exceeds the limit, hence one more delivery.
		int expectedDeliveries = DELIVERY_LIMIT + 1;
		CountDownLatch latch = new CountDownLatch(expectedDeliveries);
		container.setQueueNames(QUEUE);
		container.setMessageListener(message -> {
			deliveries.incrementAndGet();
			latch.countDown();
			throw new RuntimeException("intentional: force a requeue");
		});
		container.afterPropertiesSet();
		container.start();

		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		try {
			template.convertAndSend(QUEUE, "foo");

			assertThat(latch.await(30, TimeUnit.SECONDS))
					.withFailMessage("the message was not redelivered up to the delivery limit")
					.isTrue();

			Message deadLettered = template.receive(DLQ, 30_000);
			assertThat(deadLettered)
					.withFailMessage("the message was redelivered indefinitely instead of being dead-lettered")
					.isNotNull();
			assertThat(deadLettered.getBody()).asString().isEqualTo("foo");
		}
		finally {
			container.stop();
		}

		// The delivery is requeued (and counted) after each failure, until the limit is exceeded.
		assertThat(deliveries).hasValue(expectedDeliveries);
	}

}
