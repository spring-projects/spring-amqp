/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Gary Russell
 * @since 1.7.6
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class RabbitTemplateMPPIntegrationTests {

	private static final String QUEUE = "mpp.tests";

	private static final String REPLIES = "mpp.tests.replies";

	@ClassRule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(QUEUE, REPLIES);

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private Config config;

	@AfterClass
	public static void tearDown() {
		brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testMPPsAppliedDirectReplyToContainerTests() {
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertTrue("before MPP not called", this.config.beforeMppCalled);
		assertTrue("after MPP not called", this.config.afterMppCalled);
	}

	@Test
	public void testMPPsAppliedDirectReplyToTests() {
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertTrue("before MPP not called", this.config.beforeMppCalled);
		assertTrue("after MPP not called", this.config.afterMppCalled);
	}

	@Test
	public void testMPPsAppliedTemporaryReplyQueueTests() {
		this.template.setUseTemporaryReplyQueues(true);
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertTrue("before MPP not called", this.config.beforeMppCalled);
		assertTrue("after MPP not called", this.config.afterMppCalled);
	}

	@Test
	public void testMPPsAppliedReplyContainerTests() {
		this.template.setReplyAddress(REPLIES);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.config.cf());
		try {
			container.setQueueNames(REPLIES);
			container.setMessageListener(this.template);
			container.setAfterReceivePostProcessors(this.config.afterMPP());
			container.afterPropertiesSet();
			container.start();
			this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
			assertTrue("before MPP not called", this.config.beforeMppCalled);
			assertTrue("after MPP not called", this.config.afterMppCalled);
		}
		finally {
			container.stop();
		}
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		private boolean beforeMppCalled;

		private boolean afterMppCalled;

		@Bean
		public CachingConnectionFactory cf() {
			return new CachingConnectionFactory(brokerIsRunning.getConnectionFactory());
		}

		@Bean
		public RabbitTemplate template() {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cf());
			rabbitTemplate.setRoutingKey(QUEUE);
			rabbitTemplate.setBeforePublishPostProcessors(m -> {
				this.beforeMppCalled = true;
				return m;
			});
			rabbitTemplate.setAfterReceivePostProcessors(afterMPP());
			return rabbitTemplate;
		}

		@Bean
		public MessagePostProcessor afterMPP() {
			return m -> {
				this.afterMppCalled = true;
				return m;
			};
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory cf = new SimpleRabbitListenerContainerFactory();
			cf.setConnectionFactory(cf());
			return cf;
		}

		@RabbitListener(queues = QUEUE)
		public byte[] foo(byte[] in) {
			return in;
		}

	}

}
