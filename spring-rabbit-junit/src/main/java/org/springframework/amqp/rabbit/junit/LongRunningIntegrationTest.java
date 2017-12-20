/*
 * Copyright 2013-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to prevent long running tests from running on every build; set environment
 * variable RUN_LONG_INTEGRATION_TESTS on a CI nightly build to ensure coverage.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.2.1
 *
 */
public class LongRunningIntegrationTest extends TestWatcher {

	private final static Log logger = LogFactory.getLog(LongRunningIntegrationTest.class);

	public static final String RUN_LONG_INTEGRATION_TESTS = "RUN_LONG_INTEGRATION_TESTS";

	private boolean shouldRun = false;

	public LongRunningIntegrationTest() {
		this(RUN_LONG_INTEGRATION_TESTS);
	}

	/**
	 * Check using a custom variable/property name.
	 * @param property the variable/property name.
	 * @since 2.0.2
	 */
	public LongRunningIntegrationTest(String property) {
		for (String value: new String[] { System.getenv(property), System.getProperty(property) }) {
			if (Boolean.parseBoolean(value)) {
				this.shouldRun = true;
				break;
			}
		}
	}

	@Override
	public Statement apply(Statement base, Description description) {
		if (!this.shouldRun) {
			logger.info("Skipping long running test " + description.toString());
		}
		Assume.assumeTrue(this.shouldRun);
		return super.apply(base, description);
	}

	/**
	 * Return true if the test should run.
	 * @return true to run.
	 */
	public boolean isShouldRun() {
		return this.shouldRun;
	}

}
