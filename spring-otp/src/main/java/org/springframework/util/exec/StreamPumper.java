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

package org.springframework.util.exec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Copies all data from an input stream to an output stream.
 *
 * @author thomas.haas@softwired-inc.com
 */
public class StreamPumper implements Runnable {

    // TODO: make SIZE and SLEEP instance variables.
    // TODO: add a status flag to note if an error occured in run.

    private final static int SLEEP = 5;
    private final static int SIZE = 128;
    private InputStream is;
    private OutputStream os;


    /**
     * Create a new stream pumper.
     *
     * @param is input stream to read data from
     * @param os output stream to write data to.
     */
    public StreamPumper(InputStream is, OutputStream os) {
        this.is = is;
        this.os = os;
    }


    /**
     * Copies data from the input stream to the output stream.
     *
     * Terminates as soon as the input stream is closed or an error occurs.
     */
    public void run() {
        final byte[] buf = new byte[SIZE];

        int length;
        try {
            while ((length = is.read(buf)) > 0) {
                os.write(buf, 0, length);
                try {
                    Thread.sleep(SLEEP);
                } catch (InterruptedException e) {}
            }
        } catch(IOException e) {}
    }
}
