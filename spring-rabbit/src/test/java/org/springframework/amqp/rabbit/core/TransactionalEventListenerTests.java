/*
 * Copyright 2021-2025 the original author or authors.
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 2.2.16
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class TransactionalEventListenerTests {

	@Test
	void txCommits(@Autowired Config config, @Autowired AtomicBoolean committed,
			@Autowired Channel channel) throws IOException {

		config.publish();
		assertThat(committed.get()).isTrue();
		verify(channel).txCommit();
	}

	@Configuration(proxyBeanMethods = false)
	@EnableTransactionManagement
	public static class Config {

		@Autowired
		ApplicationEventPublisher publisher;

		@Autowired
		RabbitTemplate template;

		@Bean
		AtomicBoolean committed() {
			return new AtomicBoolean();
		}

		@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
		public void el(String in) {
			this.template.convertAndSend("test");
		}

		@Bean
		RabbitTemplate template(ConnectionFactory cf) {
			RabbitTemplate template = new RabbitTemplate(cf);
			template.setChannelTransacted(true);
			return template;
		}

		@Bean
		Channel channel() {
			return mock(Channel.class);
		}

		@Bean
		ConnectionFactory cf(Channel channel) {
			ConnectionFactory cf = mock(ConnectionFactory.class);
			Connection conn = mock(Connection.class);
			given(conn.isOpen()).willReturn(true);
			given(cf.createConnection()).willReturn(conn);
			given(conn.createChannel(true)).willReturn(channel);
			given(channel.isOpen()).willReturn(true);
			return cf;
		}

		@Transactional
		public void publish() {
			publisher.publishEvent("test");
		}

		@SuppressWarnings("serial")
		@Bean
		PlatformTransactionManager transactionManager(AtomicBoolean committed) {
			return new AbstractPlatformTransactionManager() {

				@Override
				protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
				}

				@Override
				protected Object doGetTransaction() throws TransactionException {
					return new Object();
				}

				@Override
				protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
					committed.set(true);
				}

				@Override
				protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
				}
			};
		}

	}

}
