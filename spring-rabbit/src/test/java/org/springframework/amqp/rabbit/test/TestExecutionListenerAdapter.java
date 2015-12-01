/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;

/**
 * Base class for {@link TestExecutionListener}s.
 *
 * @author Gary Russell
 * @since 1.5.3
 *
 */
public class TestExecutionListenerAdapter implements TestExecutionListener {

	@Override
	public void beforeTestClass(TestContext testContext) throws Exception {
	}

	@Override
	public void prepareTestInstance(TestContext testContext) throws Exception {
	}

	@Override
	public void beforeTestMethod(TestContext testContext) throws Exception {
	}

	@Override
	public void afterTestMethod(TestContext testContext) throws Exception {
	}

	@Override
	public void afterTestClass(TestContext testContext) throws Exception {
	}

}
