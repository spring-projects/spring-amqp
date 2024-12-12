/*
 * Copyright 2021-2024 the original author or authors.
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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.rabbitmq.client.Channel;

/**
 * @author Gary Russell
 * @since 2.2.21
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = { "async1", "async2" })
@DirtiesContext
public class AsyncReplyToTests {

	@Test
	void ackSingleWhenFatalSMLC(@Autowired Config config, @Autowired RabbitListenerEndpointRegistry registry,
			@Autowired RabbitTemplate template, @Autowired RabbitAdmin admin) throws IOException, InterruptedException {

		template.send("async1", MessageBuilder.withBody("\"foo\"".getBytes()).andProperties(
				MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.setReplyTo("nowhere")
						.build())
				.build());
		template.send("async1", MessageBuilder.withBody("junk".getBytes()).andProperties(
				MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.setReplyTo("nowhere")
						.build())
				.build());
		assertThat(config.smlcLatch.await(10, TimeUnit.SECONDS)).isTrue();
		registry.getListenerContainer("smlc").stop();
		assertThat(admin.getQueueInfo("async1").getMessageCount()).isEqualTo(1);
	}

	 @Test
	 void ackSingleWhenFatalDMLC(@Autowired Config config, @Autowired RabbitListenerEndpointRegistry registry,
			@Autowired RabbitTemplate template, @Autowired RabbitAdmin admin) throws IOException, InterruptedException {

		template.send("async2", MessageBuilder.withBody("\"foo\"".getBytes()).andProperties(
				MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.setReplyTo("nowhere")
						.build())
				.build());
		template.send("async2", MessageBuilder.withBody("junk".getBytes()).andProperties(
				MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.setReplyTo("nowhere")
						.build())
				.build());
		assertThat(config.dmlcLatch.await(10, TimeUnit.SECONDS)).isTrue();
		registry.getListenerContainer("dmlc").stop();
		assertThat(admin.getQueueInfo("async2").getMessageCount()).isEqualTo(0);
	 }

	@Configuration
	@EnableRabbit
	static class Config {

		volatile CountDownLatch smlcLatch = new CountDownLatch(1);

		volatile CountDownLatch dmlcLatch = new CountDownLatch(1);

		@RabbitListener(id = "smlc", queues = "async1", containerFactory = "smlcf")
		CompletableFuture<String> listen1(String in, Channel channel) {
			return new CompletableFuture<>();
		}

		@RabbitListener(id = "dmlc", queues = "async2", containerFactory = "dmlcf")
		CompletableFuture<String> listen2(String in, Channel channel) {
			CompletableFuture<String> future = new CompletableFuture<>();
			future.complete("test");
			return future;
		}

		@Bean
		MessageConverter converter() {
			return new Jackson2JsonMessageConverter();
		}

		@Bean
		ConnectionFactory cf() throws IOException, TimeoutException {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		SimpleRabbitListenerContainerFactory smlcf(ConnectionFactory cf, MessageConverter converter) {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(cf);
			factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
			factory.setMessageConverter(converter);
			factory.setErrorHandler(new ConditionalRejectingErrorHandler() {

				@Override
				public void handleError(Throwable t) {
					smlcLatch.countDown();
					super.handleError(t);
				}

			});
			return factory;
		}

		@Bean
		DirectRabbitListenerContainerFactory dmlcf(ConnectionFactory cf, MessageConverter converter) {
			DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
			factory.setConnectionFactory(cf);
			factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
			factory.setMessageConverter(converter);
			factory.setErrorHandler(new ConditionalRejectingErrorHandler() {

				@Override
				public void handleError(Throwable t) {
					dmlcLatch.countDown();
					super.handleError(t);
				}

			});
			return factory;
		}

		@Bean
		RabbitTemplate template(ConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

		@Bean
		RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

	}
}
