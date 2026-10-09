/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.UUID;

/**
 * Generates names using {@link UUID#randomUUID()}. (e.g.
 * "f20c818a-006b-4416-bf91-643590fedb0e").
 *
 * @author Gary Russell
 *
 * @since 2.1
 */
public class UUIDNamingStrategy implements NamingStrategy {

	/**
	 * The default instance.
	 */
	public static final UUIDNamingStrategy DEFAULT = new UUIDNamingStrategy();

	@Override
	public String generateName() {
		return UUID.randomUUID().toString();
	}

}
