/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
