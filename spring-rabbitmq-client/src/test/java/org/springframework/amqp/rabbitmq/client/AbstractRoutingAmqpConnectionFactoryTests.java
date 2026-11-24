/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbitmq.client;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * @author Ngoc Nhan
 *
 * @since 4.2
 */
public class AbstractRoutingAmqpConnectionFactoryTests {

	@Test
	public void setTargetConnectionFactoriesShouldRejectNullValues() {

		Map<Object, AmqpConnectionFactory> factories = new HashMap<>(1);
		factories.put(Boolean.FALSE, null);
		AbstractRoutingAmqpConnectionFactory factory = new AbstractRoutingAmqpConnectionFactory() {

			@Override
			protected @Nullable Object determineCurrentLookupKey() {
				return null;
			}

		};

		assertThatIllegalArgumentException()
				.isThrownBy(() -> factory.setTargetConnectionFactories(null))
				.withMessage("'targetConnectionFactories' must not be null.");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> factory.setTargetConnectionFactories(factories))
				.withMessage("'targetConnectionFactories' cannot have null values.");
	}

}
