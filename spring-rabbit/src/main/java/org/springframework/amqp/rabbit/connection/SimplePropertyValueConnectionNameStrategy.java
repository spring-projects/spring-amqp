/*
 * Copyright 2018-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

/**
 * A {@link ConnectionNameStrategy} that returns the value of a (required) property. If
 * the property does not exist, the connection will be given the name of the property.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public class SimplePropertyValueConnectionNameStrategy implements ConnectionNameStrategy, EnvironmentAware {

	private final String propertyName;

	private @Nullable String propertyValue;

	private @Nullable Environment environment;

	public SimplePropertyValueConnectionNameStrategy(String propertyName) {
		Assert.notNull(propertyName, "'propertyName' cannot be null");
		this.propertyName = propertyName;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Override
	public String obtainNewConnectionName(ConnectionFactory connectionFactory) {
		if (this.propertyValue == null) {
			if (this.environment != null) {
				this.propertyValue = this.environment.getProperty(this.propertyName);
			}
			if (this.propertyValue == null) {
				this.propertyValue = this.propertyName;
			}
		}
		return this.propertyValue;
	}

}
