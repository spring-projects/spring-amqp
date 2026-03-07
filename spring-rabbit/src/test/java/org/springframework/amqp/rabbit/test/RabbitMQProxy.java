/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RabbitMQ proxy for simulating network issues.
 * <p>
 * Accepts only a single connection. Can be started and closed multiple times.
 * </p>
 *
 * @author Alexei Sischin
 * @since 4.1.0
 */
public class RabbitMQProxy implements AutoCloseable, Runnable {

	private final String host;

	private final String targetHost;

	private final int targetPort;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final AtomicBoolean suspended = new AtomicBoolean(false);

	private int port;

	private volatile CountDownLatch startedLatch;

	private volatile ServerSocket serverSocket;

	private volatile Thread acceptThread;

	private volatile Socket clientSocket;

	private volatile Socket targetSocket;

	private volatile Thread c2tThread;

	private volatile Thread t2cThread;

	public RabbitMQProxy() {
		this("localhost", 0, "localhost", 5672);
	}

	public RabbitMQProxy(String targetHost, int targetPort) {
		this("localhost", 0, targetHost, targetPort);
	}

	public RabbitMQProxy(String host, int port, String targetHost, int targetPort) {
		this.host = host;
		this.port = port;
		this.targetHost = targetHost;
		this.targetPort = targetPort;
	}

	/**
	 * Start receiving incoming connections.
	 * @return proxy listen port.
	 */
	public synchronized int start() throws IOException {
		if (!this.running.compareAndSet(false, true)) {
			throw new IllegalStateException("Has already been started");
		}

		this.startedLatch = new CountDownLatch(1);

		this.serverSocket = new ServerSocket(this.port, 0, InetAddress.getByName(this.host));

		this.acceptThread = new Thread(this, "rabbitmq-socket-proxy");
		this.acceptThread.setDaemon(true);
		this.acceptThread.start();

		this.port = this.serverSocket.getLocalPort();
		return this.port;
	}

	public void awaitStarted() throws InterruptedException, TimeoutException {
		if (!this.running.get()) {
			throw new IllegalStateException("Has not been started");
		}
		if (!this.startedLatch.await(10, TimeUnit.SECONDS)) {
			throw new TimeoutException("Start await timeout exceeded");
		}
	}

	/**
	 * Suspend forwarding (connections stay open; bytes are read and discarded).
	 */
	public void suspend() {
		this.suspended.set(true);
	}

	/**
	 * Resume forwarding.
	 */
	public void resume() {
		this.suspended.set(false);
	}

	@Override
	public void run() {
		this.startedLatch.countDown();
		while (!Thread.interrupted()) {
			try {
				Socket incoming = this.serverSocket.accept();

				if (this.clientSocket != null && !this.clientSocket.isClosed()) {
					RabbitMQProxy.safeClose(incoming);
					continue;
				}

				Socket outgoing = new Socket(targetHost, targetPort);

				this.clientSocket = incoming;
				this.targetSocket = outgoing;

				this.c2tThread = new Thread(() -> pump(incoming, outgoing), "rabbitmq-socket-proxy-c2t");
				this.t2cThread = new Thread(() -> pump(outgoing, incoming), "rabbitmq-socket-proxy-t2c");
				this.c2tThread.setDaemon(true);
				this.t2cThread.setDaemon(true);
				this.c2tThread.start();
				this.t2cThread.start();
			}
			catch (IOException e) {
				break;
			}
		}

		RabbitMQProxy.safeClose(this.serverSocket);
		this.closeActiveConnections();
	}

	/**
	 * Close all connections and clean up.
	 */
	@Override
	public synchronized void close() {
		if (!this.running.compareAndSet(true, false)) {
			throw new IllegalStateException("Has already been closed");
		}

		if (this.acceptThread != null) {
			this.acceptThread.interrupt();
		}

		RabbitMQProxy.safeClose(this.serverSocket);
		this.closeActiveConnections();
	}

	private void pump(Socket from, Socket to) {
		try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
			byte[] buf = new byte[16 * 1024];
			while (!Thread.interrupted() && !from.isClosed() && !to.isClosed()) {
				int n = in.read(buf);
				if (n == -1) {
					break;
				}

				if (this.suspended.get()) {
					continue;
				}

				out.write(buf, 0, n);
				out.flush();
			}
		} catch (IOException ignored) {
			// Any IO error closes the active connection.
		} finally {
			this.closeActiveConnections();
		}
	}

	private synchronized void closeActiveConnections() {
		RabbitMQProxy.safeClose(this.clientSocket);
		this.clientSocket = null;

		RabbitMQProxy.safeClose(this.targetSocket);
		this.targetSocket = null;

		if (this.c2tThread != null) {
			this.c2tThread.interrupt();
			this.c2tThread = null;
		}

		if (this.t2cThread != null) {
			this.t2cThread.interrupt();
			this.t2cThread = null;
		}
	}

	private static void safeClose(Closeable ss) {
		if (ss == null) return;
		try {
			ss.close();
		} catch (IOException ignored) {
		}
	}

}
