/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test classes annotated with this will not run if an environment variable or system
 * property (default {@code RUN_LONG_INTEGRATION_TESTS}) is not present or does not have
 * the value that {@link Boolean#parseBoolean(String)} evaluates to {@code true}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.2
 */
@ExtendWith(LongRunningIntegrationTestCondition.class)
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LongRunning {

	/**
	 * The name of the variable/property used to determine whether long-running tests
	 * should run.
	 * @return the name of the variable/property.
	 */
	String value() default LongRunningIntegrationTestCondition.RUN_LONG_INTEGRATION_TESTS;

}
