/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.amqp.rabbitmq.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 4.0
 */
@ContextConfiguration
public class RabbitAmqpAdminTests extends RabbitAmqpTestBase {

	@Autowired
	@Qualifier("ds")
	Declarables declarables;

	@Test
	void verifyBeanDeclarations() {
		CompletableFuture<Void> publishFutures =
				CompletableFuture.allOf(
						template.convertAndSend("e1", "k1", "test1"),
						template.convertAndSend("e2", "k2", "test2"),
						template.convertAndSend("e2", "k2", "test3"),
						template.convertAndSend("e3", "k3", "test4"),
						template.convertAndSend("e4", "k4", "test5"));
		assertThat(publishFutures).succeedsWithin(Duration.ofSeconds(20));

		assertThat(template.receiveAndConvert("q1")).succeedsWithin(Duration.ofSeconds(20)).isEqualTo("test1");
		assertThat(template.receiveAndConvert("q2")).succeedsWithin(Duration.ofSeconds(20)).isEqualTo("test2");
		assertThat(template.receiveAndConvert("q2")).succeedsWithin(Duration.ofSeconds(20)).isEqualTo("test3");
		assertThat(template.receiveAndConvert("q3")).succeedsWithin(Duration.ofSeconds(20)).isEqualTo("test4");
		assertThat(template.receiveAndConvert("q4")).succeedsWithin(Duration.ofSeconds(20)).isEqualTo("test5");

		assertThat(declarables.getDeclarablesByType(Queue.class))
				.hasSize(1)
				.extracting(Queue::getName)
				.contains("q4");
		assertThat(declarables.getDeclarablesByType(Exchange.class))
				.hasSize(1)
				.extracting(Exchange::getName)
				.contains("e4");
		assertThat(declarables.getDeclarablesByType(Binding.class))
				.hasSize(1)
				.extracting(Binding::getDestination)
				.contains("q4");
	}

	@ParameterizedTest
	@CsvSource(textBlock = """
			q1,classic
			quorum-queue,quorum
			""")
	void shouldReturnExpectedQueueType(String queueName, String expectedQueueType) {

		assertThat(admin.getQueueInfo(queueName)).isNotNull()
				.extracting(QueueInformation::getType)
				.isEqualTo(expectedQueueType);
	}

	@Test
	void shouldDeclareClassicQueueWithGeneratedName() {

		Queue queue = admin.declareQueue();

		assertThat(queue).isNotNull()
				.extracting(Queue::getActualName)
				.isNotNull();

		assertThat(admin.getQueueInfo(queue.getActualName())).isNotNull()
				.extracting(QueueInformation::getType)
				.isEqualTo("classic");
	}

	@Test
	void shouldReturnNullForNonExistentQueue() {
		assertThat(admin.getQueueInfo("non-existent-queue")).isNull();
		assertThat(admin.getQueueProperties("non-existent-queue")).isNull();
	}

	@Test
	void declareExchangesQueuesAndBindings() {

		DirectExchange exchange1 = new DirectExchange("e1");
		DirectExchange exchange2 = new DirectExchange("e2");
		Queue q1 = new Queue("q1");
		Queue q2 = new Queue("q1");

		admin.declareExchanges(exchange1, exchange2);
		admin.declareQueues(q1, q2);
		admin.declareBindings(
				BindingBuilder.bind(q1).to(exchange1).with("k1"),
				BindingBuilder.bind(q2).to(exchange2).with("k2")
		);

		assertThat(admin.getQueueInfo("q1")).isNotNull();
		assertThat(admin.getQueueInfo("q2")).isNotNull();

		assertThat(admin.deleteExchange("e1")).isTrue();
		assertThat(admin.deleteExchange("e2")).isTrue();
	}

	@Test
	void declareExchangesQueuesAndBindingsShouldDoNothingWhenNullOrEmpty() {

		assertThatNoException()
				.isThrownBy(() -> {

					admin.declareExchanges((Exchange[]) null);
					admin.declareQueues((Queue[]) null);
					admin.declareBindings((Binding[]) null);

					admin.declareExchanges();
					admin.declareQueues();
					admin.declareBindings();
				});
	}

	@Test
	void declareExchangesQueuesAndBindingsShouldThrowIllegalArgumentExceptionWhenContainNullElements() {

		assertThatIllegalArgumentException()
				.isThrownBy(() -> admin.declareExchanges((Exchange) null));

		assertThatIllegalArgumentException()
				.isThrownBy(() -> admin.declareQueues((Queue) null));

		assertThatIllegalArgumentException()
				.isThrownBy(() -> admin.declareBindings((Binding) null));
	}

	@Configuration
	public static class Config {

		@Bean
		DirectExchange e1() {
			return new DirectExchange("e1");
		}

		@Bean
		Queue q1() {
			return new Queue("q1");
		}

		@Bean
		Binding b1() {
			return BindingBuilder.bind(q1()).to(e1()).with("k1");
		}

		@Bean
		Declarables es() {
			return new Declarables(
					new DirectExchange("e2"),
					new DirectExchange("e3"));
		}

		@Bean
		Declarables qs() {
			return new Declarables(
					new Queue("q2"),
					new Queue("q3"));
		}

		@Bean
		Declarables bs() {
			return new Declarables(
					new Binding("q2", Binding.DestinationType.QUEUE, "e2", "k2", null),
					new Binding("q3", Binding.DestinationType.QUEUE, "e3", "k3", null));
		}

		@Bean
		Declarables ds() {
			return new Declarables(
					new DirectExchange("e4"),
					new Queue("q4"),
					new Binding("q4", Binding.DestinationType.QUEUE, "e4", "k4", null));
		}

		@Bean
		Queue quorum() {
			return QueueBuilder.durable("quorum-queue").quorum().build();
		}

	}

}
