/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Gary Russell
 * @author Heng Zhang
 * @author Artem Bilan
 *
 * @since 3.0
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = "test.batchQueue")
@DirtiesContext
public class BatchMessagingMessageListenerAdapterTests {

	@Test
	void compatibleMethod() throws Exception {
		Method method = getClass().getDeclaredMethod("listen", List.class);
		BatchMessagingMessageListenerAdapter adapter = new BatchMessagingMessageListenerAdapter(this, method, false,
				null, null);
		assertThat(TestUtils.<Type>getPropertyValue(adapter, "messagingMessageConverter.inferredArgumentType"))
				.isEqualTo(String.class);
		Method badMethod = getClass().getDeclaredMethod("listen", String.class);
		assertThatIllegalStateException().isThrownBy(() ->
				new BatchMessagingMessageListenerAdapter(this, badMethod, false, null, null)
		).withMessageStartingWith("Mis-configuration");
	}

	public void listen(String in) {
	}

	public void listen(List<String> in) {
	}

	@Test
	public void errorMsgConvert(@Autowired BatchMessagingMessageListenerAdapterTests.Config config,
			@Autowired RabbitTemplate template) throws Exception {

		Message message = MessageBuilder.withBody("""
						{
							"name" : "Tom",
							"age" : 18
						}
						""".getBytes()).andProperties(
						MessagePropertiesBuilder.newInstance()
								.setContentType("application/json")
								.setReplyTo("nowhere")
								.build())
				.build();

		Message errorMessage = MessageBuilder.withBody("".getBytes()).andProperties(
						MessagePropertiesBuilder.newInstance()
								.setContentType("application/json")
								.setReplyTo("nowhere")
								.build())
				.build();

		for (int i = 0; i < config.count; i++) {
			template.send("test.batchQueue", message);
			template.send("test.batchQueue", errorMessage);
		}

		assertThat(config.countDownLatch.await(config.count * 1000L, TimeUnit.SECONDS)).isTrue();
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		volatile int count = 5;

		volatile CountDownLatch countDownLatch = new CountDownLatch(count);

		@RabbitListener(
				queues = "test.batchQueue",
				containerFactory = "batchListenerContainerFactory"
		)
		public void listen(List<Model> list) {
			for (Model model : list) {
				countDownLatch.countDown();
			}

		}

		@Bean
		ConnectionFactory cf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean(name = "batchListenerContainerFactory")
		public RabbitListenerContainerFactory<SimpleMessageListenerContainer> rc(ConnectionFactory connectionFactory) {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory);
			factory.setPrefetchCount(1);
			factory.setConcurrentConsumers(1);
			factory.setBatchListener(true);
			factory.setBatchSize(3);
			factory.setConsumerBatchEnabled(true);

			factory.setMessageConverter(new JacksonJsonMessageConverter());

			return factory;
		}

		@Bean
		RabbitTemplate template(ConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

	}

	public static class Model {

		String name;

		String age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getAge() {
			return age;
		}

		public void setAge(String age) {
			this.age = age;
		}

	}

}
