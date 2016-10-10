/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.amqp.core;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

/**
 * @author Gary Russell
 * @since 1.5.3
 *
 */
public class QueueNameTests {

	@Test
	public void testAnonymous() {
		AnonymousQueue q = new AnonymousQueue();
		assertThat(q.getName(), startsWith("spring.gen-"));
		q = new AnonymousQueue(new AnonymousQueue.Base64UrlNamingStrategy("foo-"));
		assertThat(q.getName(), startsWith("foo-"));
		q = new AnonymousQueue(AnonymousQueue.UUIDNamingStrategy.DEFAULT);
		assertTrue("Not a UUID: " + q.getName(),
				Pattern.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", q.getName()));
	}

}
