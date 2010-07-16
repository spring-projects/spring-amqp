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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.admin.RabbitBrokerAdmin;
import org.springframework.amqp.rabbit.admin.RabbitStatus;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.erlang.OtpIOException;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.test.context.transaction.TransactionConfigurationAttributes;

/**
 * A TestExeuctionListener that will start/reset the RabbitMQ node before the methods in the
 * TestClass are executed.  If the node was started before the test methods were executed,
 * the node will be stopped after the test methods are executed .
 * 
 * @author Mark Pollack
 */
public class RabbitTestExecutionListener extends AbstractTestExecutionListener{
	
	private static final Log logger = LogFactory.getLog(RabbitTestExecutionListener.class);

	private RabbitBrokerAdmin rabbitAdminTemplate;

	private boolean startedNode;


	@Override
	public void beforeTestClass(TestContext testContext) throws Exception {				
		initializeRabbitAdminTemplate(testContext);	
		recycleBrokerApp();
	}
	
	@Override
	public void afterTestClass(TestContext testContext) throws Exception {
		if (startedNode) {
			rabbitAdminTemplate.stopNode();
			startedNode = false;
		}
	}

	private void recycleBrokerApp() {
		try {
			RabbitStatus status = rabbitAdminTemplate.getStatus();
			logger.debug(status);
			rabbitAdminTemplate.stopBrokerApplication();
			rabbitAdminTemplate.resetNode();
			rabbitAdminTemplate.startBrokerApplication();
		}
		catch (OtpIOException e) {
			// Can't connect because broker node isn't running.
			rabbitAdminTemplate.startNode();
			startedNode = true;
			//TODO - need to wait on output for 'broker running'
			try {
				Thread.sleep(2000);
			}
			catch (InterruptedException e1) {
				logger.error("Error waiting for broker to start");
			}
		}
	}

	/**
	 * Retrieves the {@link TransactionConfigurationAttributes} for the
	 * specified {@link Class class} which may optionally declare or inherit a
	 * {@link TransactionConfiguration @TransactionConfiguration}. If a
	 * {@link TransactionConfiguration} annotation is not present for the
	 * supplied class, the <em>default values</em> for attributes defined in
	 * {@link TransactionConfiguration} will be used instead.
	 * @param clazz the Class object corresponding to the test class for which
	 * the configuration attributes should be retrieved
	 * @return a new TransactionConfigurationAttributes instance
	 */
	private void initializeRabbitAdminTemplate(TestContext testContext) {		
		
		//Could potentially get the information from the BeanFactory by implementing a BeanFactoryPostProcessor
		if (this.rabbitAdminTemplate == null) {
			Class<?> clazz = testContext.getTestClass();
			Class<RabbitConfiguration> annotationType = RabbitConfiguration.class;
			RabbitConfiguration config = clazz.getAnnotation(annotationType);
			if (logger.isDebugEnabled()) {
				logger.debug("Retrieved @RabbitConfiguration [" + config + "] for test class [" + clazz + "]");
			}
			String hostname;
			String username;
			String password;
			if (config != null) {
				hostname = config.hostname();
				username = config.username();
				password = config.password();
			}
			else {
				hostname = (String) AnnotationUtils.getDefaultValue(annotationType, "hostname");
				username = (String) AnnotationUtils.getDefaultValue(annotationType, "username");
				password = (String) AnnotationUtils.getDefaultValue(annotationType, "password");	
			}
			if (logger.isDebugEnabled()) {
				logger.debug(
						String.format("Retrieved hostname=[%s] username=[%s], password=[%s] for class [%s]",
								hostname, username, password, clazz));
			}
			SingleConnectionFactory connectionFactory;
			if (hostname.equals("localhost")) {
				//This will try to get the local host name
				connectionFactory = new SingleConnectionFactory();
			}
			else {
				connectionFactory = new SingleConnectionFactory(hostname);
			}
			connectionFactory.setUsername(username);
			connectionFactory.setPassword(password);
			rabbitAdminTemplate = new RabbitBrokerAdmin(connectionFactory);			
		}		
	}

}
