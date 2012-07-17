/*
 * Copyright (c) 2011-2012 by the original author(s).
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

import java.util.concurrent.CountDownLatch;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Gary Russell
 */
public class TestListener implements MessageListener {

  private CountDownLatch latch;

  private Object id;

  public TestListener(int count) {
    latch = new CountDownLatch(count);
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public Object getId() {
	return id;
}

public void onMessage(Message message) {
    System.out.println("MESSAGE: " + message);
    System.out.println("BODY: " + new String(message.getBody()));
    this.id = message.getMessageProperties().getMessageId();
    latch.countDown();
  }

}
