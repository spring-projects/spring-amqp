/*
 * Copyright 2002-2025 the original author or authors.
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

/**
 * Enumeration for the message delivery mode. Can be persistent or
 * non-persistent. Use the method 'toInt' to get the appropriate value
 * that is used by the AMQP protocol instead of the ordinal() value when
 * passing into AMQP APIs.
 *
 * @author Mark Pollack
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public enum MessageDeliveryMode {

	/**
	 * Non persistent.
	 */
	NON_PERSISTENT,

	/**
	 * Persistent.
	 */
	PERSISTENT;

	public static int toInt(MessageDeliveryMode mode) {
		return switch (mode) {
			case NON_PERSISTENT -> 1;
			case PERSISTENT -> 2;
		};
	}

	public static MessageDeliveryMode fromInt(int modeAsNumber) {
		return switch (modeAsNumber) {
			case 1 -> NON_PERSISTENT;
			case 2 -> PERSISTENT;
			default -> throw new IllegalArgumentException("Unknown mode: " + modeAsNumber);
		};
	}

}
