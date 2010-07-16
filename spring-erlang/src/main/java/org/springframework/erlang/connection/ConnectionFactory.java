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

package org.springframework.erlang.connection;

import java.io.IOException;
import java.net.UnknownHostException;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;

/**
 * An interface based ConnectionFactory for creating {@link OtpConnection}s.
 * 
 * <p>NOTE: The Rabbit API contains a ConnectionFactory class (same name).
 * 
 * @author Mark Pollack
 */
public interface ConnectionFactory {

	OtpConnection createConnection() throws UnknownHostException, OtpAuthException, IOException; 

}
