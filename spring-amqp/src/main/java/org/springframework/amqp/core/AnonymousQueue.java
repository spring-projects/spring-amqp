/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.core;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.springframework.util.Assert;
import org.springframework.util.Base64Utils;

/**
 * Represents an anonymous, non-durable, exclusive, auto-delete queue.
 * The name is a UUID by default but it can be modified by providing a naming
 * strategy.
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class AnonymousQueue extends Queue {

	/**
	 * Construct a queue with a UUID name.
	 */
	public AnonymousQueue() {
		this((Map<String, Object>) null);
	}

	/**
	 * Construct a queue with a UUID name with the supplied arguments.
	 * @param arguments the arguments.
	 */
	public AnonymousQueue(Map<String, Object> arguments) {
		super(UUID.randomUUID().toString(),  false, true, true, arguments);
	}

	/**
	 * Construct a queue with a name provided by the supplied naming strategy.
	 * @param namingStrategy the naming strategy.
	 * @since 1.5.3
	 */
	public AnonymousQueue(NamingStrategy namingStrategy) {
		this(namingStrategy, null);
	}

	/**
	 * Construct a queue with a name provided by the supplied naming strategy with the
	 * supplied arguments.
	 * @param namingStrategy the naming strategy.
	 * @param arguments the arguments.
	 * @since 1.5.3
	 */
	public AnonymousQueue(NamingStrategy namingStrategy, Map<String, Object> arguments) {
		super(namingStrategy.generateName(), false, true, true, arguments);
	}

	/**
	 * A strategy to name anonymous queues
	 * @since 1.5.3
	 *
	 */
	@FunctionalInterface
	public interface NamingStrategy {

		String generateName();

	}

	/**
	 * Generates names with the form {@code <prefix><base64url>} where
	 * 'prefix' is 'spring.gen-' by default;
	 * the 'base64url' String is generated from a UUID. The base64 alphabet
	 * is the "URL and Filename Safe Alphabet"; see RFC-4648. Trailing padding
	 * characters (@code =) are removed.
	 * @since 1.5.3
	 */
	public static class Base64UrlNamingStrategy implements NamingStrategy {

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
			ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
			bb.putLong(uuid.getMostSignificantBits())
			  .putLong(uuid.getLeastSignificantBits());
			// Convert to base64 and remove trailing =
			return this.prefix + Base64Utils.encodeToUrlSafeString(bb.array())
									.replaceAll("=", "");
		}

	}

}
