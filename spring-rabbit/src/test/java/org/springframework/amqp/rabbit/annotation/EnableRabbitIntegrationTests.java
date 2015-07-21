/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ErrorHandler;

/**
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 * @since 1.4
 */
@ContextConfiguration(classes = EnableRabbitIntegrationTests.EnableRabbitConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EnableRabbitIntegrationTests {

	@ClassRule
	public static final BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(
			"test.simple", "test.header", "test.message", "test.reply", "test.sendTo", "test.sendTo.reply",
			"test.sendTo.spel", "test.sendTo.reply.spel",
			"test.invalidPojo");

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private CountDownLatch errorHandlerLatch;

	@Autowired
	private AtomicReference<Throwable> errorHandlerError;

	@Autowired
	private String tagPrefix;

	@Test
	public void autoDeclare() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("auto.exch", "auto.rk", "foo"));
	}

	@Test
	public void autoDeclareFanout() {
		assertEquals("FOOFOO", rabbitTemplate.convertSendAndReceive("auto.exch.fanout", "", "foo"));
	}

	@Test
	public void autoDeclareAnon() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("auto.exch", "auto.anon.rk", "foo"));
	}

	@Test
	public void simpleEndpoint() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("test.simple", "foo"));
	}

	@Test
	public void multiListener() {
		Bar bar = new Bar();
		bar.field = "bar";
		assertEquals("BAR: bar", rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", bar));
		Baz baz = new Baz();
		baz.field = "baz";
		assertEquals("BAZ: baz", rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", baz));
		Qux qux = new Qux();
		qux.field = "qux";
		assertEquals("QUX: qux: multi.rk", rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", qux));
	}

	@Test
	public void endpointWithHeader() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.header", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
	}

	@Test
	public void endpointWithMessage() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.message", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
	}

	@Test
	public void endpointWithComplexReply() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("foo", "fooValue");
		Message request = MessageTestUtils.createTextMessage("content", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.reply", request);
		assertEquals("Wrong reply", "content", MessageTestUtils.extractText(reply));
		assertEquals("Wrong foo header", "fooValue", reply.getMessageProperties().getHeaders().get("foo"));
		assertThat((String) reply.getMessageProperties().getHeaders().get("bar"), Matchers.startsWith(tagPrefix));
	}

	@Test
	public void simpleEndpointWithSendTo() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo", "bar");
		int n = 0;
		Object result = null;
		while ((result = rabbitTemplate.receiveAndConvert("test.sendTo.reply")) == null && n++ < 100) {
			Thread.sleep(100);
		}
		assertTrue(n < 100);
		assertNotNull(result);
		assertEquals("BAR", result);
	}

	@Test
	public void simpleEndpointWithSendToSpel() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo.spel", "bar");
		int n = 0;
		Object result = null;
		while ((result = rabbitTemplate.receiveAndConvert("test.sendTo.reply.spel")) == null && n++ < 100) {
			Thread.sleep(100);
		}
		assertTrue(n < 100);
		assertNotNull(result);
		assertEquals("BARbar", result);
	}

	@Test
	public void testInvalidPojoConversion() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("test.invalidPojo", "bar");

		assertTrue(this.errorHandlerLatch.await(10, TimeUnit.SECONDS));
		Throwable throwable = this.errorHandlerError.get();
		assertNotNull(throwable);
		assertThat(throwable, instanceOf(AmqpRejectAndDontRequeueException.class));
		assertThat(throwable.getCause(), instanceOf(ListenerExecutionFailedException.class));
		assertThat(throwable.getCause().getCause(),
				instanceOf(org.springframework.amqp.support.converter.MessageConversionException.class));
		assertThat(throwable.getCause().getCause().getCause(),
				instanceOf(org.springframework.messaging.converter.MessageConversionException.class));
		assertThat(throwable.getCause().getCause().getCause().getMessage(),
				containsString("Failed to convert message payload 'bar' to 'java.util.Date'"));
	}

	public static class MyService {

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(value = "auto.declare", autoDelete = "true"),
				exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
				key = "auto.rk")
		)
		public String handleWithDeclare(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(value = "auto.declare.fanout", autoDelete = "true"),
				exchange = @Exchange(value = "auto.exch.fanout", autoDelete = "true", type="fanout"))
		)
		public String handleWithFanout(String foo) {
			return foo.toUpperCase() + foo.toUpperCase();
		}

		@RabbitListener(bindings = {
				@QueueBinding(
					value = @Queue(),
					exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
					key = "auto.anon.rk")}
		)
		public String handleWithDeclareAnon(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queues = "test.simple")
		public String capitalize(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queues = "test.header")
		public String capitalizeWithHeader(@Payload String content, @Header String prefix) {
			return prefix + content.toUpperCase();
		}

		@RabbitListener(queues = "test.message")
		public String capitalizeWithMessage(org.springframework.messaging.Message<String> message) {
			return message.getHeaders().get("prefix") + message.getPayload().toUpperCase();
		}

		@RabbitListener(queues = "test.reply")
		public org.springframework.messaging.Message<?> reply(String payload, @Header String foo,
				@Header(AmqpHeaders.CONSUMER_TAG) String tag) {
			return MessageBuilder.withPayload(payload)
					.setHeader("foo", foo).setHeader("bar", tag).build();
		}

		@RabbitListener(queues = "test.sendTo")
		@SendTo("test.sendTo.reply")
		public String capitalizeAndSendTo(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queues = "test.sendTo.spel")
		@SendTo("#{spelReplyTo}")
		public String capitalizeAndSendToSpel(String foo) {
			return foo.toUpperCase() + foo;
		}

		@RabbitListener(queues = "test.invalidPojo")
		public void handleIt(Date body) {

		}

	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfig {

		private int increment;

		@Bean
		public String spelReplyTo() {
			return "test.sendTo.reply.spel";
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			return factory;
		}

		@Bean
		public String tagPrefix() {
			return UUID.randomUUID().toString();
		}

		@Bean
		public ConsumerTagStrategy consumerTagStrategy() {
			return new ConsumerTagStrategy() {

				@Override
				public String createConsumerTag(String queue) {
					return tagPrefix() + increment++;
				}
			};
		}

		@Bean
		public CountDownLatch errorHandlerLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		public AtomicReference<Throwable> errorHandlerError() {
			return new AtomicReference<Throwable>();
		}

		@Bean
		public ErrorHandler errorHandler() {
			ErrorHandler handler = Mockito.spy(new ConditionalRejectingErrorHandler());
			Mockito.doAnswer(new Answer<Object>() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					try {
						return invocation.callRealMethod();
					}
					catch (Throwable e) {
						errorHandlerError().set(e);
						errorHandlerLatch().countDown();
						throw e;
					}
				}
			}).when(handler).handleError(Mockito.any(Throwable.class));
			return handler;
		}

		@Bean
		public MyService myService() {
			return new MyService();
		}

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate rabbitTemplate() {
			return new RabbitTemplate(rabbitConnectionFactory());
		}

		@Bean
		public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
			return new RabbitAdmin(connectionFactory);
		}

		@Bean
		public MultiListenerBean multiListener() {
			return new MultiListenerBean();
		}

	}

	@RabbitListener(bindings = @QueueBinding
			(value = @Queue,
			exchange = @Exchange(value = "multi.exch", autoDelete = "true"),
			key = "multi.rk"))
	static class MultiListenerBean {

		@RabbitHandler
		public String bar(Bar bar) {
			return "BAR: " + bar.field;
		}

		@RabbitHandler
		public String baz(Baz baz) {
			return "BAZ: " + baz.field;
		}

		@RabbitHandler
		public String qux(@Header("amqp_receivedRoutingKey") String rk, @Payload Qux qux) {
			return "QUX: " + qux.field + ": " + rk;
		}

	}

	@SuppressWarnings("serial")
	static class Foo implements Serializable {

		String field;

	}

	@SuppressWarnings("serial")
	static class Bar extends Foo implements Serializable {

	}

	@SuppressWarnings("serial")
	static class Baz extends Foo implements Serializable {

	}

	@SuppressWarnings("serial")
	static class Qux extends Foo implements Serializable {

	}

}
