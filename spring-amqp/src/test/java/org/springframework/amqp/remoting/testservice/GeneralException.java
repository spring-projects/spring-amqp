package org.springframework.amqp.remoting.testservice;

public class GeneralException extends RuntimeException {
	private static final long serialVersionUID = 1763252570120227426L;

	public GeneralException(String message, Throwable cause) {
		super(message, cause);
	}

	public GeneralException(String message) {
		super(message);
	}

}
