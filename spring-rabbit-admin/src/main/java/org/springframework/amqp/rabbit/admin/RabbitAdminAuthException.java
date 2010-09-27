package org.springframework.amqp.rabbit.admin;

import org.springframework.erlang.OtpAuthException;

@SuppressWarnings("serial")
public class RabbitAdminAuthException extends OtpAuthException {

	public RabbitAdminAuthException(String message, OtpAuthException cause) {
		super(message, (com.ericsson.otp.erlang.OtpAuthException) cause.getCause());
	}

}
