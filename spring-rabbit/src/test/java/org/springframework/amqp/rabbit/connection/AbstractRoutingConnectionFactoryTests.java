/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

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
public class AbstractRoutingConnectionFactoryTests {

	@Test
	public void setTargetConnectionFactoriesShouldRejectNullValues() {

		Map<Object, ConnectionFactory> factories = new HashMap<>(1);
		factories.put(Boolean.FALSE, null);
		AbstractRoutingConnectionFactory factory = new AbstractRoutingConnectionFactory() {

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
