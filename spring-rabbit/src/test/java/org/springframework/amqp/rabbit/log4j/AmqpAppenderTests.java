/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.log4j;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    locations = {
        "org.springframework.amqp.rabbit.log4j"
    },
    loader = AnnotationConfigContextLoader.class
)
public class AmqpAppenderTests {

  @Autowired
  ApplicationContext applicationContext;

  Logger log;
  SimpleMessageListenerContainer listenerContainer;

  @Before
  public void setUp() {
    log = Logger.getLogger(getClass());
    listenerContainer = applicationContext.getBean(SimpleMessageListenerContainer.class);
  }

  @After
  public void tearDown() {
    listenerContainer.shutdown();
  }

  @Test
  public void testAppender() throws InterruptedException {
    TestListener testListener = (TestListener) applicationContext.getBean("testListener", 4);
    listenerContainer.setMessageListener(testListener);
    listenerContainer.start();

    Logger log = Logger.getLogger(getClass());

    log.debug("This is a DEBUG message");
    log.info("This is an INFO message");
    log.warn("This is a WARN message");
    log.error("This is an ERROR message", new RuntimeException("Test exception"));

    testListener.getLatch().await(5, TimeUnit.SECONDS);
  }

  @Test
  public void testAppenderWithProps() throws InterruptedException {
    TestListener testListener = (TestListener) applicationContext.getBean("testListener", 4);
    listenerContainer.setMessageListener(testListener);
    listenerContainer.start();

    MDC.put("someproperty", "property.value");
    log.debug("This is a DEBUG message with properties");
    log.info("This is an INFO message with properties");
    log.warn("This is a WARN message with properties");
    log.error("This is an ERROR message with properties", new RuntimeException("Test exception"));
    MDC.remove("someproperty");

    testListener.getLatch().await(5, TimeUnit.SECONDS);
  }

}
