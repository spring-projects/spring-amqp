/*
 * Copyright 2017-2025 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 1.7.6
 *
 */
@RabbitAvailable(queues = {
		RabbitTemplateMPPIntegrationTests.QUEUE,
		RabbitTemplateMPPIntegrationTests.REPLIES })
@SpringJUnitConfig
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class RabbitTemplateMPPIntegrationTests {

	public static final String QUEUE = "mpp.tests";

	public static final String REPLIES = "mpp.tests.replies";

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private Config config;

	@Test
	public void testMPPsAppliedDirectReplyToContainerTests() {
		this.config.afterMppCalled = 0;
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertThat(this.config.beforeMppCalled).as("before MPP not called").isTrue();
		assertThat(this.config.afterMppCalled).as("after MPP not called").isEqualTo(1);
	}

	@Test
	public void testMPPsAppliedDirectReplyToTests() {
		this.config.afterMppCalled = 0;
		this.template.setUseDirectReplyToContainer(false);
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertThat(this.config.beforeMppCalled).as("before MPP not called").isTrue();
		assertThat(this.config.afterMppCalled).as("after MPP not called").isEqualTo(1);
	}

	@Test
	public void testMPPsAppliedTemporaryReplyQueueTests() {
		this.config.afterMppCalled = 0;
		this.template.setUseDirectReplyToContainer(false);
		this.template.setUseTemporaryReplyQueues(true);
		this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
		assertThat(this.config.beforeMppCalled).as("before MPP not called").isTrue();
		assertThat(this.config.afterMppCalled).as("after MPP not called").isEqualTo(1);
	}

	@Test
	public void testMPPsAppliedReplyContainerTests() {
		this.config.afterMppCalled = 0;
		this.template.setReplyAddress(REPLIES);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.config.cf());
		try {
			container.setQueueNames(REPLIES);
			container.setReceiveTimeout(10);
			container.setMessageListener(this.template);
			container.afterPropertiesSet();
			container.start();
			this.template.sendAndReceive(new Message("foo".getBytes(), new MessageProperties()));
			assertThat(this.config.beforeMppCalled).as("before MPP not called").isTrue();
			assertThat(this.config.afterMppCalled).as("after MPP not called").isEqualTo(1);
		}
		finally {
			container.stop();
		}
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		boolean beforeMppCalled;

		int afterMppCalled;

		@Bean
		public CachingConnectionFactory cf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning()
					.getConnectionFactory());
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
				this.afterMppCalled++;
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
