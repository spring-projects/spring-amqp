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

package org.springframework.amqp.rabbit.config;

import org.junit.After;
import org.junit.Before;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 *
 */
public class QueueParserPlaceholderTests extends QueueParserTests {

	@Before
	@Override
	public void setUpDefaultBeanFactory() throws Exception {
		beanFactory = new GenericXmlApplicationContext(new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@After
	public void closeBeanFactory() throws Exception {
		if (beanFactory != null) {
			((ConfigurableApplicationContext) beanFactory).close();
		}
	}

}
