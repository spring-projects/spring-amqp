/*
 * Copyright 2002-2010 the original author or authors.
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

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.junit.runners.model.InitializationError;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.TestExecutionListener;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;

/**
 * A specialized JUnit4 class runner so that we can add a RabbitTestExecutionListener.
 * 
 * The RabbitTestExecutionListener needs to run before the DependencyInjectionTestExecutionListener
 * so that the RabbitMQ broker application and be 'reset' or the RabbitMQ node 
 * started before the application context is created so that RabbitMQ broker configuration can 
 * execute when the applicatin context is created.
 * 
 * 
 * @author Mark Pollack
 *
 */
public class SpringRabbitJUnit4ClassRunner extends SpringJUnit4ClassRunner {

	public SpringRabbitJUnit4ClassRunner(Class<?> clazz)
			throws InitializationError {
		super(clazz);
	}
	

	/**
	 * Creates a new {@link TestContextManager} for the supplied test class and
	 * the configured <em>default <code>ContextLoader</code> class name</em>.
	 * Can be overridden by subclasses.
	 * 
	 * @param clazz the test class to be managed
	 * @see #getDefaultContextLoaderClassName(Class)
	 */
	protected TestContextManager createTestContextManager(Class<?> clazz) {
		TestContextManager mgr = new TestContextManager(clazz, getDefaultContextLoaderClassName(clazz));
		
		Field executionListenersField = ReflectionUtils.findField(TestContextManager.class, "testExecutionListeners");
		executionListenersField.setAccessible(true);
		@SuppressWarnings("unchecked")
		ArrayList<TestExecutionListener> executionListeners = 
			(ArrayList<TestExecutionListener>) ReflectionUtils.getField(executionListenersField, mgr);
		ArrayList<TestExecutionListener> newExecutionListeners = new ArrayList<TestExecutionListener>();
		newExecutionListeners.add(new RabbitTestExecutionListener());
		for (TestExecutionListener testExecutionListener : executionListeners) {
			newExecutionListeners.add(testExecutionListener);
		}
		ReflectionUtils.setField(executionListenersField, mgr, newExecutionListeners);
		
		//puts to the back of the list - need it before context is created as broker configuration instructions are
		//executed as part of the context creation.
		//mgr.registerTestExecutionListeners(new RabbitTestExecutionListener());
		
		return mgr;
	}


}
