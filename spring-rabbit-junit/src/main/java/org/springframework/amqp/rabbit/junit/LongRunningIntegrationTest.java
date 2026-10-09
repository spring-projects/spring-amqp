/*
 * Copyright 2013-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to prevent long-running tests from running on every build; set environment
 * variable RUN_LONG_INTEGRATION_TESTS on a CI nightly build to ensure coverage.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.2.1
 *
 * @deprecated since 4.0 in favor of JUnit 5 {@link LongRunning}.
 */
@Deprecated(since = "4.0", forRemoval = true)
public class LongRunningIntegrationTest extends TestWatcher {

	private static final Log logger = LogFactory.getLog(LongRunningIntegrationTest.class); // NOSONAR - lower case

	public static final String RUN_LONG_INTEGRATION_TESTS =
			LongRunningIntegrationTestCondition.RUN_LONG_INTEGRATION_TESTS;

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
		this.shouldRun = JUnitUtils.parseBooleanProperty(property);
	}

	@Override
	public Statement apply(Statement base, Description description) {
		if (!this.shouldRun) {
			logger.info("Skipping long running test " + description);
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
