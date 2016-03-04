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

package org.springframework.amqp.remoting.testservice;

/**
 * @author David Bilge
 * @author Gary Russell
 * @since 1.2
 */
public class TestServiceImpl implements TestServiceInterface {
	@Override
	public void simpleTestMethod() {
		// Do nothing
	}

	@Override
	public String simpleStringReturningTestMethod(String string) {
		return "Echo " + string;
	}

	@Override
	public void exceptionThrowingMethod() {
		throw new RuntimeException("This is an exception");
	}

	@Override
	public Object echo(Object o) {
		return o;
	}

	@Override
	public SpecialException notReallyExceptionReturningMethod() {
		throw new GeneralException("This exception should not be interpreted as a return type but be thrown instead.");
	}

	@Override
	public SpecialException actuallyExceptionReturningMethod() {
		return new SpecialException("This exception should not be thrown on the client side but just be returned!");
	}

	@Override
	public Object simulatedTimeoutMethod(Object o) {
		return null;
	}

}
