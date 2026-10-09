/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.rabbit.connection;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Utility methods for configuring connection factories.
 *
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
public final class ConnectionFactoryConfigurationUtils {

	private ConnectionFactoryConfigurationUtils() {
	}

	/**
	 * Parse the properties {@code key:value[,key:value]...} and add them to the
	 * underlying connection factory client properties.
	 * @param connectionFactory the connection factory.
	 * @param clientConnectionProperties the properties.
	 */
	public static void updateClientConnectionProperties(AbstractConnectionFactory connectionFactory,
			@Nullable String clientConnectionProperties) {

		if (clientConnectionProperties != null) {
			String[] props = clientConnectionProperties.split(",");
			if (props.length > 0) {
				Map<String, Object> clientProps =
						connectionFactory.getRabbitConnectionFactory()
								.getClientProperties();

				for (String prop : props) {
					String[] aProp = prop.split(":");
					if (aProp.length == 2) {
						clientProps.put(aProp[0].trim(), aProp[1].trim());
					}
				}
			}
		}
	}

}
