/*
 * Copyright 2017 the original author or authors.
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
 * @since 2.0.2
 *
 */
@ExtendWith(LongRunningIntegrationTestCondition.class)
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LongRunning {

	/**
	 * The name of the variable/property used to determine whether long runnning tests
	 * should run.
	 * @return the name of the variable/property.
	 */
	String value() default LongRunningIntegrationTest.RUN_LONG_INTEGRATION_TESTS;

}
