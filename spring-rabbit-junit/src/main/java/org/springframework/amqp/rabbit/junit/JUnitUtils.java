/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

/**
 * Utility methods for JUnit rules and conditions.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public final class JUnitUtils {

	private JUnitUtils() {
		super();
	}

	/**
	 * Return the parsed value if the provided property exists in the environment or system properties.
	 * @param property the property name.
	 * @return the parsed property value if it exists, false otherwise.
	 */
	public static boolean parseBooleanProperty(String property) {
		for (String value: new String[] { System.getenv(property), System.getProperty(property) }) {
			if (Boolean.parseBoolean(value)) {
				return true;
			}
		}
		return false;
	}

}
