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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * A JUnit method &#064;Rule that changes the logger level for a set of classes
 * while a test method is running. Useful for performance or scalability tests
 * where we don't want to generate a large log in a tight inner loop.
 *
 * @author Dave Syer
 *
 */
public class Log4jLevelAdjuster implements MethodRule {

	private static final Log logger = LogFactory.getLog(Log4jLevelAdjuster.class);

	private final Class<?>[] classes;

	private final Level level;

	public Log4jLevelAdjuster(Level level, Class<?>... classes) {
		this.level = level;
		this.classes = classes;
	}

	public Statement apply(final Statement base, FrameworkMethod method, Object target) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				logger.debug("Overriding log level setting for: " + Arrays.asList(classes));
				Map<Class<?>, Level> oldLevels = new HashMap<Class<?>, Level>();
				for (Class<?> cls : classes) {
					oldLevels.put(cls, LogManager.getLogger(cls).getLevel());
					LogManager.getLogger(cls).setLevel(level);
				}
				try {
					base.evaluate();
				}
				finally {
					logger.debug("Restoring log level setting for: " + Arrays.asList(classes));
					for (Class<?> cls : classes) {
						LogManager.getLogger(cls).setLevel(oldLevels.get(cls));
					}
				}
			}
		};
	}

}
