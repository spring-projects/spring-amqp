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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ericsson.otp.erlang.OtpConnection;

/**
 * @author Mark Pollack
 */
public class ConnectionFactoryUtils {

	private static final Log logger = LogFactory.getLog(ConnectionFactoryUtils.class);


	/**
	 * Release the given Connection by closing it.
	 */
	public static void releaseConnection(OtpConnection con, ConnectionFactory cf) {
		if (con == null) {
			return;
		}
		try {
			con.close();
		}
		catch (Throwable ex) {
			logger.debug("Could not close Otp Connection", ex);
		}
	}
	
}
