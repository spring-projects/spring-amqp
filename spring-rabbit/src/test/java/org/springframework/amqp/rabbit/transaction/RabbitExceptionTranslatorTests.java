/*
 * Copyright 2013-present the original author or authors.
 */

package org.springframework.amqp.rabbit.transaction;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;

import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpUnsupportedEncodingException;
import org.springframework.amqp.UncategorizedAmqpException;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Sergey Shcherbakov
 */
public class RabbitExceptionTranslatorTests {

	@Test
	public void testConvertRabbitAccessException() {

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new PossibleAuthenticationFailureException(new RuntimeException()))).isInstanceOf(AmqpAuthenticationException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new AmqpException(""))).isInstanceOf(AmqpException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new ShutdownSignalException(false, false, null, null))).isInstanceOf(AmqpConnectException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new ConnectException())).isInstanceOf(AmqpConnectException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new IOException())).isInstanceOf(AmqpIOException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new UnsupportedEncodingException())).isInstanceOf(AmqpUnsupportedEncodingException.class);

		assertThat(RabbitExceptionTranslator.convertRabbitAccessException(new Exception() {

			private static final long serialVersionUID = 1L;
		})).isInstanceOf(UncategorizedAmqpException.class);

	}

}
