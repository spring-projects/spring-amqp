/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.amqp.core;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

import org.springframework.util.Assert;

/**
 * Generates names with the form {@code <prefix><base64url>} where 'prefix' is
 * 'spring.gen-' by default (e.g. spring.gen-eIwaZAYgQv6LvwaDCfVTNQ); the 'base64url'
 * String is generated from a UUID. The base64 alphabet is the "URL and Filename Safe
 * Alphabet"; see RFC-4648. Trailing padding characters (@code =) are removed.
 *
 * @author Gary Russell
 *
 * @since 2.1
 */
public class Base64UrlNamingStrategy implements NamingStrategy {

	private static final int SIXTEEN = 16;

	/**
	 * The default instance - using {@code spring.gen-} as the prefix.
	 */
	public static final Base64UrlNamingStrategy DEFAULT = new Base64UrlNamingStrategy();

	private final String prefix;

	/**
	 * Construct an instance using the default prefix {@code spring.gen-}.
	 */
	public Base64UrlNamingStrategy() {
		this("spring.gen-");
	}

	/**
	 * Construct an instance using the supplied prefix.
	 * @param prefix The prefix.
	 */
	public Base64UrlNamingStrategy(String prefix) {
		Assert.notNull(prefix, "'prefix' cannot be null; use an empty String ");
		this.prefix = prefix;
	}

	@Override
	public String generateName() {
		UUID uuid = UUID.randomUUID();
		ByteBuffer bb = ByteBuffer.wrap(new byte[SIXTEEN]);
		bb.putLong(uuid.getMostSignificantBits())
		  .putLong(uuid.getLeastSignificantBits());
		// Convert to base64 and remove trailing =
		return this.prefix + Base64.getUrlEncoder().encodeToString(bb.array())
								.replaceAll("=", "");
	}

}
