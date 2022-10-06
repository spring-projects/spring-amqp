/*
 * Copyright 2015-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5
 *
 */
@RabbitAvailable(management = true)
public class RabbitRestApiTests {

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");

	private final Client rabbitRestClient;

	public RabbitRestApiTests() throws MalformedURLException, URISyntaxException {
		this.rabbitRestClient = new Client("http://localhost:15672/api/", "guest", "guest");
	}

	@AfterEach
	public void tearDown() {
		connectionFactory.destroy();
	}

	@Test
	public void testExchanges() {
		List<ExchangeInfo> list = this.rabbitRestClient.getExchanges();
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testExchangesVhost() {
		List<ExchangeInfo> list = this.rabbitRestClient.getExchanges("/");
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testBindings() {
		List<BindingInfo> list = this.rabbitRestClient.getBindings();
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testBindingsVhost() {
		List<BindingInfo> list = this.rabbitRestClient.getBindings("/");
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testQueues() {
		List<QueueInfo> list = this.rabbitRestClient.getQueues();
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testQueuesVhost() {
		List<QueueInfo> list = this.rabbitRestClient.getQueues("/");
		assertThat(list.size() > 0).isTrue();
	}

	@Test
	public void testBindingsDetail() {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("alternate-exchange", "");
		Exchange exchange1 = new DirectExchange(UUID.randomUUID().toString(), false, true, args);
		admin.declareExchange(exchange1);
		Exchange exchange2 = new DirectExchange(UUID.randomUUID().toString(), false, true, args);
		admin.declareExchange(exchange2);
		Queue queue = admin.declareQueue();
		Binding binding1 = BindingBuilder
				.bind(queue)
				.to(exchange1)
				.with("foo")
				.and(args);
		admin.declareBinding(binding1);
		Binding binding2 = BindingBuilder
				.bind(exchange2)
				.to((DirectExchange) exchange1)
				.with("bar");
		admin.declareBinding(binding2);

		List<BindingInfo> bindings = this.rabbitRestClient.getBindingsBySource("/", exchange1.getName());
		assertThat(bindings).hasSize(2);
		assertThat(bindings.get(0).getSource()).isEqualTo(exchange1.getName());
		assertThat("foo").isIn(bindings.get(0).getRoutingKey(), bindings.get(1).getRoutingKey());
		BindingInfo qout = null;
		BindingInfo eout = null;
		if (bindings.get(0).getRoutingKey().equals("foo")) {
			qout = bindings.get(0);
			eout = bindings.get(1);
		}
		else {
			eout = bindings.get(0);
			qout = bindings.get(1);
		}
		assertThat(qout.getDestinationType()).isEqualTo("queue");
		assertThat(qout.getDestination()).isEqualTo(queue.getName());
		assertThat(qout.getArguments()).isNotNull();
		assertThat(qout.getArguments().get("alternate-exchange")).isEqualTo("");

		assertThat(eout.getDestinationType()).isEqualTo("exchange");
		assertThat(eout.getDestination()).isEqualTo(exchange2.getName());

		admin.deleteExchange(exchange1.getName());
		admin.deleteExchange(exchange2.getName());
	}

	@Test
	public void testSpecificExchange() {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("alternate-exchange", "");
		Exchange exchange = new DirectExchange(UUID.randomUUID().toString(), true, true, args);
		admin.declareExchange(exchange);
		ExchangeInfo exchangeOut = this.rabbitRestClient.getExchange("/", exchange.getName());
		assertThat(exchangeOut.isDurable()).isTrue();
		assertThat(exchangeOut.isAutoDelete()).isTrue();
		assertThat(exchangeOut.getName()).isEqualTo(exchange.getName());
		assertThat(exchangeOut.getArguments()).isEqualTo(args);
		admin.deleteExchange(exchange.getName());
	}

	@Test
	public void testSpecificQueue() throws Exception {
		RabbitAdmin admin = new RabbitAdmin(connectionFactory);
		Map<String, Object> args = Collections.<String, Object>singletonMap("foo", "bar");
		Queue queue1 = QueueBuilder.nonDurable(UUID.randomUUID().toString())
				.autoDelete()
				.withArguments(args)
				.build();
		admin.declareQueue(queue1);
		Queue queue2 = QueueBuilder.durable(UUID.randomUUID().toString())
				.withArguments(args)
				.build();
		admin.declareQueue(queue2);
		Channel channel = this.connectionFactory.createConnection().createChannel(false);
		String consumer = channel.basicConsume(queue1.getName(), false, "", false, true, null, new DefaultConsumer(channel));
		QueueInfo qi = await().until(() -> this.rabbitRestClient.getQueue("/", queue1.getName()),
				info -> info.getExclusiveConsumerTag() != null && !"".equals(info.getExclusiveConsumerTag()));
		QueueInfo queueOut = this.rabbitRestClient.getQueue("/", queue1.getName());
		assertThat(queueOut.isDurable()).isFalse();
		assertThat(queueOut.isExclusive()).isFalse();
		assertThat(queueOut.isAutoDelete()).isTrue();
		assertThat(queueOut.getName()).isEqualTo(queue1.getName());
		assertThat(queueOut.getArguments()).isEqualTo(args);
		assertThat(qi.getExclusiveConsumerTag()).isEqualTo(consumer);
		channel.basicCancel(consumer);
		channel.close();

		queueOut = this.rabbitRestClient.getQueue("/", queue2.getName());
		assertThat(queueOut.isDurable()).isTrue();
		assertThat(queueOut.isExclusive()).isFalse();
		assertThat(queueOut.isAutoDelete()).isFalse();
		assertThat(queueOut.getName()).isEqualTo(queue2.getName());
		assertThat(queueOut.getArguments()).isEqualTo(args);

		admin.deleteQueue(queue1.getName());
		admin.deleteQueue(queue2.getName());
	}

	@Test
	public void testDeleteExchange() {
		String exchangeName = "testExchange";
		Exchange testExchange = new DirectExchange(exchangeName);
		ExchangeInfo info = new ExchangeInfo();
		info.setArguments(testExchange.getArguments());
		info.setAutoDelete(testExchange.isAutoDelete());
		info.setDurable(testExchange.isDurable());
		info.setType(testExchange.getType());
		this.rabbitRestClient.declareExchange("/", testExchange.getName(), info);
		ExchangeInfo exchangeToAssert = this.rabbitRestClient.getExchange("/", exchangeName);
		assertThat(exchangeToAssert.getName()).isEqualTo(testExchange.getName());
		assertThat(exchangeToAssert.getType()).isEqualTo(testExchange.getType());
		this.rabbitRestClient.deleteExchange("/", testExchange.getName());
		// 6.0.0 REST compatibility
//		assertThat(this.rabbitRestClient.getExchange("/", exchangeName)).isNull();
		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		assertThatExceptionOfType(AmqpException.class)
				.isThrownBy(() -> template.execute(channel -> channel.exchangeDeclarePassive(exchangeName)))
				.withCauseExactlyInstanceOf(IOException.class);
	}

}
