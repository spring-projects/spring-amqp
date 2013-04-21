package org.springframework.amqp.remoting.testservice;

public class SpecialException extends RuntimeException {
	private static final long serialVersionUID = 7254934411128057730L;

	public SpecialException(String message, Throwable cause) {
		super(message, cause);
	}

	public SpecialException(String message) {
		super(message);
	}

}
