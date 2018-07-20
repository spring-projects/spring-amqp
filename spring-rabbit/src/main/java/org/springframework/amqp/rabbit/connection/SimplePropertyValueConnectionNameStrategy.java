/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

/**
 * A {@link ConnectionNameStrategy} that returns the value of a (required) property.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public class SimplePropertyValueConnectionNameStrategy implements ConnectionNameStrategy, EnvironmentAware {

	private final String propertyName;

	private String propertyValue;

	public SimplePropertyValueConnectionNameStrategy(String propertyName) {
		this.propertyName = propertyName;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.propertyValue = environment.getProperty(this.propertyName);
		Assert.notNull(this.propertyValue, () -> "No property exists with name '" + this.propertyName + "'");
	}

	@Override
	public String obtainNewConnectionName(ConnectionFactory connectionFactory) {
		return this.propertyValue;
	}

}
