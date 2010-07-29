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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Class to execute processes in the background.  These processes
 * will not exit once the controlling Java process exits.  
 */
public class Background {
    /**
     * Execute the command (and its args, ala Runtime.exec), sending the
     * output && error streams to the void.
     */
    public static void exec(String[] cmd)
        throws IOException
    {
        File devNull;
        if(Os.isFamily("unix")) 
            devNull = new File("/dev/null");
        else if (Os.isFamily("windows")) 
            devNull = new File("NUL");
        else
            throw new IllegalStateException("Unhandled Java environment");
        exec(cmd, devNull, false, devNull, false);
    }

    /**
     * Execute a command (and its args, ala Runtime.exec)
     *
     * @param outFile    File to send standard out from the process to
     * @param appendOut  If true, append the file with standard out,
     *                   else truncate or create a new file
     * @param errFile    File to send standard err from the process to
     * @param appendErr  If true, append the file with standard error, 
     *                   else truncate or create a new file
     */
    public static void exec(String[] cmd, 
                            File outFile, boolean appendOut,
                            File errFile, boolean appendErr)
        throws IOException
    {
        if(Os.isFamily("unix"))
            execUnix(cmd, outFile, appendOut, errFile, appendErr);
        else if (Os.isFamily("windows"))
            execWin(cmd, outFile, appendOut, errFile, appendErr);
        else
            throw new IllegalStateException("Unhandled Java environment");
    }

    private static void execUnix(String[] cmd, 
                                 File outFile, boolean appendOut, 
                                 File errFile, boolean appendErr)
        throws IOException
    {
        StringBuffer escaped;
        String[] execCmd;

        escaped = new StringBuffer();
        for(int i=0; i<cmd.length; i++){
            escaped.append(Escape.escape(cmd[i]));
            escaped.append(" ");
        }

        execCmd = new String[] {
            "/bin/sh",
            "-c",
            escaped.toString() + 
            (appendOut == true ? ">>" : ">") + 
            Escape.escape(outFile.getAbsolutePath()) +
            " 2" + (appendErr == true ? ">>" : " >") +
            Escape.escape(errFile.getAbsolutePath()) + 
            " </dev/null &"
        };

        Process p = Runtime.getRuntime().exec(execCmd);
        try {
            p.waitFor();
        } catch(Exception exc){
            throw new IOException("Unable to properly background process: " +
                                  exc.getMessage());
        }
    }

    private static void execWin(String[] cmd, 
                                File outFile, boolean appendOut, 
                                File errFile, boolean appendErr)
        throws IOException
    {
        ArrayList<String> tmpCmd = new ArrayList<String>();

        tmpCmd.add("cmd");
        tmpCmd.add("/c");
        tmpCmd.add("start");
        tmpCmd.add("/b");
        tmpCmd.add("\"\"");
        tmpCmd.add("/MIN");
        for(int i=0; i<cmd.length; i++){
            tmpCmd.add(cmd[i]);
        }
        tmpCmd.add((appendOut == true ? ">>" : ">") + 
            Escape.escape(outFile.getAbsolutePath()));
        tmpCmd.add((outFile.equals(errFile) ? " 2&" : " 2") + 
            (appendErr == true ? ">>" : " >") +
            Escape.escape(errFile.getAbsolutePath())); 

        Runtime.getRuntime().exec((String [])tmpCmd.toArray(cmd));
    }

    public static void main(String[] args) throws Exception {
        Background.exec(new String[] {"javaq", "foo bar", "bar" },
                        new File("garfo"), true, new File("barfo"), true);
    }
}
