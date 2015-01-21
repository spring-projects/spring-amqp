/*
 * Copyright 2002-2015 the original author or authors.
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

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * A simple interface that is used to wrap access to the {@code OtpConnection} class
 * in order to support caching of {@code OtpConnection}s via method interception.
 *
 * Note:  The surface area of the API is all that is required to implement administrative
 * functionality for the Spring AMQP admin project.
 * To access the underlying {@code OtpConnection}, use the method {@code getTargetConnection}
 * on the interface {@code ConnectionProxy} that is implemented by {@code DefaultConnection}.
 *
 * @author Mark Pollack
 * @author aArtem Bilan
 */
public interface Connection {

	/**
	 *  Close the connection to the remote node.
	 */
	void close();

    /**
     * Send an RPC request to the remote Erlang node. This convenience function
     * creates the following message and sends it to 'rex' on the remote node:
     * <pre class="code">
     * { self, { call, Mod, Fun, Args, user } }
     * </pre>
     * <p>
     * Note that this method has unpredictable results if the remote node is not
     * an Erlang node.
     * @param mod  the name of the Erlang module containing the function to  be called.
     * @param fun  the name of the function to call.
     * @param args a list of Erlang terms, to be used as arguments to the function.
     * @exception java.io.IOException if the connection is not active or a communication
     *                    error occurs.
     */
	void sendRPC(final String mod, final String fun, final OtpErlangList args) throws IOException;

    /**
     * Receive an RPC reply from the remote Erlang node. This convenience
     * function receives a message from the remote node, and expects it to have
     * the following format:
     * <pre class="code">
     * { rex, Term }
     * </pre>
     * @return the second element of the tuple if the received message is a
     *         two-tuple, otherwise null. No further error checking is
     *         performed.
     * @exception java.io.IOException if the connection is not active or a communication
     *                    error occurs.
     * @exception OtpErlangExit if an exit signal is received from a process on the
     *                    peer node.
     * @exception OtpAuthException if the remote node sends a message containing an
     *                    invalid cookie.
     */
	OtpErlangObject receiveRPC() throws IOException, OtpErlangExit, OtpAuthException;

	/**
	 * Send an RPC request to the remote Erlang node and receive result.
	 * The implementation must ensure {@code synchronized} mode of this method since
	 * the underlying {@code OtpConnection} isn't thread-safe.
	 * @param mod  the name of the Erlang module containing the function to  be called.
	 * @param fun  the name of the function to call.
	 * @param args a list of Erlang terms, to be used as arguments to the function.
	 * @return the second element of the tuple if the received message is a
	 *         two-tuple, otherwise null. No further error checking is
	 *         performed.
	 * @exception java.io.IOException if the connection is not active or a communication
	 *                    error occurs.
	 * @exception OtpErlangExit if an exit signal is received from a process on the
	 *                    peer node.
	 * @exception OtpAuthException if the remote node sends a message containing an
	 *                    invalid cookie.
	 * @since 1.5
	 */
	OtpErlangObject sendAndReceiveRPC(final String mod, final String fun, final OtpErlangList args)
			throws IOException, OtpErlangExit, OtpAuthException;

}
