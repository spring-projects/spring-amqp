/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.lang.reflect.Method;

import org.springframework.util.ClassUtils;

/**
 * Utility methods for log appenders.
 *
 * @author Gary Russell
 * @since 1.5.6
 *
 * @deprecated in favor of {@code ConnectionFactoryConfigurationUtils}.
 *
 */
@Deprecated
public final class LogAppenderUtils {

	private static final Method util;

	static {
		Method method;
		try {
			Class<?> utils = ClassUtils.forName(
					"org.springframework.amqp.rabbit.connection.ConnectionFactoryConfigurationUtils",
					LogAppenderUtils.class.getClassLoader());
			Class<?> abstractCF = ClassUtils.forName(
					"org.springframework.amqp.rabbit.connection.AbstractConnectionFactory",
					LogAppenderUtils.class.getClassLoader());
			method = utils.getDeclaredMethod("updateClientConnectionProperties", abstractCF, String.class);
		}
		catch (Exception e) {
			method = null;
		}
		util = method;
	}

	private LogAppenderUtils() {
		super();
	}

	/**
	 * Parse the properties {@code key:value[,key:value]...} and add them to the
	 * connection factory client properties.
	 * @param connectionFactory the connection factory.
	 * @param clientConnectionProperties the properties.
	 */
	public static void updateClientConnectionProperties(Object connectionFactory,
			String clientConnectionProperties) {

		try {
			util.invoke(null, connectionFactory, clientConnectionProperties);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to set properties", e);
		}
	}

}
