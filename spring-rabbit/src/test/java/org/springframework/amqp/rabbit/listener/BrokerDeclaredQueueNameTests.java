/*
 * Copyright 2018-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
@SpringJUnitConfig
@RabbitAvailable
@DirtiesContext
public class BrokerDeclaredQueueNameTests {

	@Autowired
	private CachingConnectionFactory cf;

	@Autowired
	private CountDownLatch latch1;

	@Autowired
	private CountDownLatch latch2;

	@Autowired
	private CountDownLatch latch3;

	@Autowired
	private CountDownLatch latch4;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Autowired
	private AtomicReference<Message> message;

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private SimpleMessageListenerContainer smlc;

	@Autowired
	private DirectMessageListenerContainer dmlc;

	private static Level savedLevel;

	@BeforeAll
	public static void setUp() {
		Logger logger = (Logger) LogManager
				.getLogger("org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer");
		savedLevel = logger.getLevel();
		logger.setLevel(Level.DEBUG);
	}

	@AfterAll
	public static void tearDown() {
		Logger logger = (Logger) LogManager
				.getLogger("org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer");
		logger.setLevel(savedLevel);
	}

	@Test
	public void testBrokerNamedQueueSMLC() throws Exception {
		testBrokerNamedQueue(this.smlc, this.latch1, this.latch2, this.queue1);
	}

	@Test
	public void testBrokerNamedQueueDMLC() throws Exception {
		testBrokerNamedQueue(this.dmlc, this.latch3, this.latch4, this.queue2);
	}

	private void testBrokerNamedQueue(AbstractMessageListenerContainer container,
			CountDownLatch firstLatch, CountDownLatch secondLatch, Queue queue) throws Exception {

		container.start();
		String firstActualName = queue.getActualName();
		this.message.set(null);
		this.template.convertAndSend(firstActualName, "foo");
		assertThat(firstLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.message.get().getBody()).isEqualTo("foo".getBytes());
		final CountDownLatch newConnectionLatch = new CountDownLatch(2);
		this.cf.addConnectionListener(c -> newConnectionLatch.countDown());
		this.cf.stop();
		assertThat(newConnectionLatch.await(10, TimeUnit.SECONDS)).isTrue();
		String secondActualName = queue.getActualName();
		assertThat(secondActualName).isNotEqualTo(firstActualName);
		this.message.set(null);
		this.template.convertAndSend(secondActualName, "bar");
		assertThat(secondLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.message.get().getBody()).isEqualTo("bar".getBytes());
		container.stop();
	}

	@Configuration
	public static class Config {

		@Bean
		public AtomicReference<Message> message() {
			return new AtomicReference<>();
		}

		@Bean
		public CountDownLatch latch1() {
			return new CountDownLatch(1);
		}

		@Bean
		public CountDownLatch latch2() {
			return new CountDownLatch(2);
		}

		@Bean
		public Queue queue1() {
			return new Queue("", false, true, true);
		}

		@Bean
		public CachingConnectionFactory cf() {
			return new CachingConnectionFactory(
					RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(cf());
		}

		@Bean
		public AbstractMessageListenerContainer container() {
			AbstractMessageListenerContainer container = new SimpleMessageListenerContainer(cf());
			container.setQueues(queue1());
			container.setMessageListener(m -> {
				message().set(m);
				latch1().countDown();
				latch2().countDown();
			});
			container.setFailedDeclarationRetryInterval(100);
			container.setMissingQueuesFatal(false);
			container.setRecoveryInterval(100);
			container.setAutoStartup(false);
			return container;
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(cf());
		}

		@Bean
		public CountDownLatch latch3() {
			return new CountDownLatch(1);
		}

		@Bean
		public CountDownLatch latch4() {
			return new CountDownLatch(2);
		}

		@Bean
		public Queue queue2() {
			return new Queue("", false, true, true);
		}

		@Bean
		public AbstractMessageListenerContainer dmlc() {
			AbstractMessageListenerContainer container = new DirectMessageListenerContainer(cf());
			container.setQueues(queue2());
			container.setMessageListener(m -> {
				message().set(m);
				latch3().countDown();
				latch4().countDown();
			});
			container.setFailedDeclarationRetryInterval(1000);
			container.setMissingQueuesFatal(false);
			container.setRecoveryInterval(100);
			container.setAutoStartup(false);
			return container;
		}

	}

}
