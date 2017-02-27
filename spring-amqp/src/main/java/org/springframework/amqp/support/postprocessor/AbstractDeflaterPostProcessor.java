/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.support.postprocessor;

import java.util.zip.Deflater;

/**
 * Base class for post processors based on {@link Deflater}.
 * @author Gary Russell
 *
 * @since 1.4.2
 *
 */
public abstract class AbstractDeflaterPostProcessor extends AbstractCompressingPostProcessor {

	protected int level = Deflater.BEST_SPEED;

	public AbstractDeflaterPostProcessor() {
		super();
	}

	public AbstractDeflaterPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	/**
	 * Set the deflater compression level.
	 * @param level the level (default {@link Deflater#BEST_SPEED}
	 * @see Deflater
	 */
	public void setLevel(int level) {
		this.level = level;
	}

}
