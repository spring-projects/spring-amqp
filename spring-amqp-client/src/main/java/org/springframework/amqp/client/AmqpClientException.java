/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client;

import java.io.Serial;

import org.apache.qpid.protonj2.client.exceptions.ClientException;

import org.springframework.amqp.AmqpException;

/**
 * The {@link AmqpException} wrapper for the checked {@link ClientException}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpClientException extends AmqpException {

	@Serial
	private static final long serialVersionUID = 1L;

	public AmqpClientException(ClientException cause) {
		super(cause);
	}

	public AmqpClientException(String message, ClientException cause) {
		super(message, cause);
	}

}
