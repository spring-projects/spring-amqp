/*
 * Copyright 2013-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.transaction;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;

import org.junit.Test;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;

import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Sergey Shcherbakov
 */
public class RabbitExceptionTranslatorTests {

	@Test
	public void testConvertRabbitAccessException() {

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new PossibleAuthenticationFailureException(new RuntimeException())),
				instanceOf(AmqpAuthenticationException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new AmqpException("")),
				instanceOf(AmqpException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new ShutdownSignalException(false, false, null, null)),
				instanceOf(AmqpConnectException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new ConnectException()),
				instanceOf(AmqpConnectException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new IOException()),
				instanceOf(AmqpIOException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new UnsupportedEncodingException()),
				instanceOf(AmqpUnsupportedEncodingException.class));

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new Exception() {
				private static final long serialVersionUID = 1L;
			}), instanceOf(UncategorizedAmqpException.class));

	}

}
