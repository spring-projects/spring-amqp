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

package org.springframework.amqp.rabbit.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class EnvironmentAvailable extends TestWatcher {

	private static Log logger = LogFactory.getLog(EnvironmentAvailable.class);

	private static final String DEFAULT_ENVIRONMENT_KEY = "ENVIRONMENT";

	private final String key;

	public EnvironmentAvailable(String key) {
		this.key = key;
	}

	public EnvironmentAvailable() {
		this(DEFAULT_ENVIRONMENT_KEY);
	}

	@Override
	public Statement apply(final Statement base, Description description) {
		logger.info("Environment: " + key + " active=" + isActive());
		Assume.assumeTrue(isActive());
		return super.apply(base, description);
	}

	public boolean isActive() {
		return System.getProperty(key) != null;
	}

}
