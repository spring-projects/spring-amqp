/*
 * Copyright 2002-2010 the original author or authors.
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

/**
 * Enumeration for the message delivery mode.  Can be persistent or 
 * non persistent.  Use the method 'toInt' to get the appropriate value
 * that is used the he AMQP protocol instead of the ordinal() value when
 * passing into AMQP APIs.
 * 
 * @author Mark Pollack
 *
 */
public enum MessageDeliveryMode {

	NON_PERSISTENT, PERSISTENT;

	public static int toInt(MessageDeliveryMode mode) {
		switch (mode) {
		case NON_PERSISTENT: {
			return 1;
		}
		case PERSISTENT: {
			return 2;
		}
		default: {
			return -1;
		}
		}
	}

	public static MessageDeliveryMode fromInt(int modeAsNumber) {
		switch (modeAsNumber) {
		case 1: {
			return NON_PERSISTENT;
		}
		case 2: {
			return PERSISTENT;
		}
		default: {
			return null;
		}
		}
	}
	
}
