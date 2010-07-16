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

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.ByteArrayOutputStream;
import java.util.*;


/* Derived from ant exec task. All the 'backward compat with jdk1.1, 1.2'
   removed. Since jdk1.3 supports working dir, no need for scripts.

   All ant-specific code has been removed as well, this is a completely
   independent component.
   
   Costin
*/

/**
 * Runs an external program.
 *
 * @author thomas.haas@softwired-inc.com
 */
public class Execute {
    /** Invalid exit code. **/
    public final static int INVALID = Integer.MAX_VALUE;

    private String[] cmdl = null;
    private String[] env = null;
    private int exitValue = INVALID;
    private ExecuteStreamHandler streamHandler;
    private ExecuteWatchdog watchdog;
    private File workingDirectory = null;
    private boolean newEnvironment = false;

    private static Vector procEnvironment = null;

    /**
     * Find the list of environment variables for this process.
     */
    public static synchronized Vector getProcEnvironment() {
        if (procEnvironment != null) return procEnvironment;

        procEnvironment = new Vector();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Execute exe = new Execute(new PumpStreamHandler(out));
            exe.setCommandline(getProcEnvCommand());
            // Make sure we do not recurse forever
            exe.setNewenvironment(true);
            int retval = exe.execute();
            if ( retval != 0 ) {
                // Just try to use what we got
            }

            BufferedReader in =
                new BufferedReader(new StringReader(out.toString()));
            String var = null;
            String line, lineSep = System.getProperty("line.separator");
            while ((line = in.readLine()) != null) {
                if (line.indexOf('=') == -1) {
                    // Chunk part of previous env var (UNIX env vars can
                    // contain embedded new lines).
                    if (var == null) {
                        var = lineSep + line;
                    }
                    else {
                        var += lineSep + line;
                    }
                }
                else {
                    // New env var...append the previous one if we have it.
                    if (var != null) {
                        procEnvironment.addElement(var);
                    }
                    var = line;
                }
            }
            // Since we "look ahead" before adding, there's one last env var.
            procEnvironment.addElement(var);
        }
        catch (Exception exc) {
            exc.printStackTrace();
            // Just try to see how much we got
        }
        return procEnvironment;
    }

    private static String[] getProcEnvCommand() {
        if ( Os.isFamily("os/2") ) {
            // OS/2 - use same mechanism as Windows 2000
            // Not sure
            String[] cmd = {"cmd", "/c", "set" };
            return cmd;
        }
        else if ( Os.isFamily("windows") ) {
            String osname =
                System.getProperty("os.name").toLowerCase(Locale.US);
            String[] cmd = {"cmd", "/c", "set" };
            return cmd;
        }
        else if ( Os.isFamily("unix") ) {
            // Generic UNIX
            // Alternatively one could use: /bin/sh -c env
            String[] cmd = {"/usr/bin/env"};
            return cmd;
        }
        else if ( Os.isFamily("netware") ) {
            String[] cmd = {"env"};
            return cmd;
        }
        else {
            // MAC OS 9 and previous
            // TODO: I have no idea how to get it, someone must fix it
            String[] cmd = null;
            return cmd;
        }
    }

    /**
     * Creates a new execute object using <code>PumpStreamHandler</code> for
     * stream handling.
     */
    public Execute() {
        this(new PumpStreamHandler(), null);
    }


    /**
     * Creates a new execute object.
     *
     * @param streamHandler the stream handler used to handle the input and
     *        output streams of the subprocess.
     */
    public Execute(ExecuteStreamHandler streamHandler) {
        this(streamHandler, null);
    }

    /**
     * Creates a new execute object.
     *
     * @param streamHandler the stream handler used to handle the input and
     *        output streams of the subprocess.
     * @param watchdog a watchdog for the subprocess or <code>null</code> to
     *        to disable a timeout for the subprocess.
     */
    public Execute(ExecuteStreamHandler streamHandler, ExecuteWatchdog watchdog) {
        this.streamHandler = streamHandler;
        this.watchdog = watchdog;
    }


    /**
     * Returns the commandline used to create a subprocess.
     *
     * @return the commandline used to create a subprocess
     */
    public String[] getCommandline() {
        return cmdl;
    }

    public String getCommandLineString() {
        return array2string(getCommandline());
    }

    /**
     * Sets the commandline of the subprocess to launch.
     *
     * @param commandline the commandline of the subprocess to launch
     */
    public void setCommandline(String[] commandline) {
        cmdl = commandline;
    }

    /**
     * Set whether to propagate the default environment or not.
     *
     * @param newenv whether to propagate the process environment.
     */
    public void setNewenvironment(boolean newenv) {
        newEnvironment = newenv;
    }

    /**
     * Returns the environment used to create a subprocess.
     *
     * @return the environment used to create a subprocess
     */
    public String[] getEnvironment() {
        if (env == null || newEnvironment) return env;
        return patchEnvironment();
    }


    /**
     * Sets the environment variables for the subprocess to launch.
     *
     * @param commandline array of Strings, each element of which has
     * an environment variable settings in format <em>key=value</em>
     */
    public void setEnvironment(String[] env) {
        this.env = env;
    }

    /**
     * Sets the working directory of the process to execute.
     *
     * <p>This is emulated using the antRun scripts unless the OS is
     * Windows NT in which case a cmd.exe is spawned,
     * or MRJ and setting user.dir works, or JDK 1.3 and there is
     * official support in java.lang.Runtime.
     *
     * @param wd the working directory of the process.
     */
    public void setWorkingDirectory(File wd) {
        workingDirectory = wd;
    }

    // costin
    
    boolean wait=true;
    public void setWait( boolean b ) {
        wait=b;
    }

    Process process;
    /**
     * Runs a process defined by the command line and returns its exit status.
     *
     * @return the exit status of the subprocess or <code>INVALID</code>
     * @exception java.io.IOExcpetion The exception is thrown, if launching
     *            of the subprocess failed
     */
    public int execute() throws Exception {
        process =
            Runtime.getRuntime().exec( getCommandline(), getEnvironment(),
                                       workingDirectory );
        try {
            streamHandler.setProcessInputStream(process.getOutputStream());
            streamHandler.setProcessOutputStream(process.getInputStream());
            streamHandler.setProcessErrorStream(process.getErrorStream());
        } catch (IOException e) {
            process.destroy();
            throw e;
        }
        streamHandler.start();

        if (watchdog != null) watchdog.start(process,
                                             Thread.currentThread());

        if( log.isTraceEnabled() ) log.trace("Waiting process ");
        waitFor(process);

        if( log.isTraceEnabled() ) log.trace("End waiting, stop threads ");
        if (watchdog != null) watchdog.stop();
        if( log.isTraceEnabled() ) log.trace("Watchdog stopped ");
        streamHandler.stop();
        if( log.isTraceEnabled() ) log.trace("Stream handler stopped ");
        if (watchdog != null) {
            Exception ex=watchdog.getException();
            if( ex!=null )
                throw ex;
        }
        int exit= getExitValue();

        if( log.isDebugEnabled() ) {
            log.debug("Done exit=" + exit + " "  + getCommandLineString());
        }
        return exit;
    }

    private String array2string( String sa[]) {
        if( sa==null ) return "null";
        StringBuffer sb=new StringBuffer();
        for( int i=0; i<sa.length; i++ ) sb.append(sa[i]).append(" ");
        return sb.toString();
    }

    protected void waitFor(Process process) {
        try {
            process.waitFor();
            setExitValue(process.exitValue());
        } catch (InterruptedException e) {
            log.info("waitFor() interrupted ");
        }
    }

    protected void setExitValue(int value) {
        exitValue = value;
    }

    /**
     * query the exit value of the process.
     * @return the exit value, 1 if the process was killed,
     * or Project.INVALID if no exit value has been received
     */
    public int getExitValue() {
        return exitValue;
    }

    /**
     * Patch the current environment with the new values from the user.
     * @return the patched environment
     */
    private String[] patchEnvironment() {
        Vector osEnv = (Vector) getProcEnvironment().clone();
        for (int i = 0; i < env.length; i++) {
            int pos = env[i].indexOf('=');
            // Get key including "="
            String key = env[i].substring(0, pos+1);
            int size = osEnv.size();
            for (int j = 0; j < size; j++) {
                if (((String)osEnv.elementAt(j)).startsWith(key)) {
                    osEnv.removeElementAt(j);
                    break;
                }
            }
            osEnv.addElement(env[i]);
        }
        String[] result = new String[osEnv.size()];
        osEnv.copyInto(result);
        return result;
    }

    public static int execute( Vector envVars, String cmd, File baseDir ) {
        Vector v=new Vector();
        StringTokenizer st=new StringTokenizer( cmd, " " );
        while( st.hasMoreTokens() ) {
            v.addElement( st.nextElement() );
        }

        return execute( envVars, v, baseDir );
    }
    
    public static int execute( Vector envVars, Vector cmd, File baseDir) {
	return execute( envVars, cmd, baseDir, 10000 /* default time to wait */);
    }

    /** Wrapper for common execution patterns
     * @param envVars Environment variables to execute with (optional)
     * @param cmd     a vector of the commands to execute
     * @param baseDir the base directory to run from (optional) 
     * @param timeToWait milliseconds to wait for completion
     */
    public static int execute( Vector envVars, Vector cmd, File baseDir, int timeToWait) {
        try {
            // We can collect the out or provide in if needed
            ExecuteWatchdog watchdog=new ExecuteWatchdog( timeToWait );
            watchdog.setDontkill( true );
            PumpStreamHandler out=new PumpStreamHandler();
            Execute exec=new Execute(out,watchdog);

            String cmdA[]=new String[ cmd.size() ];
            cmd.toArray( cmdA );
            if( log.isDebugEnabled() ) {
                StringBuffer sb=new StringBuffer();
                for(int i=0; i<cmdA.length; i++ ) {
                    sb.append(cmdA[i] + " " );
                }
                log.debug( "Exec: " + sb.toString());
            }
            exec.setCommandline( cmdA );

            if( envVars!=null ) {
                String env[]=new String[envVars.size()];
                envVars.toArray( env );
                exec.setEnvironment( env );
            }

            exec.setNewenvironment( false );
            if( baseDir!=null)
                exec.setWorkingDirectory( baseDir );

            exec.execute();
            int status=exec.getExitValue();
            log.debug("Exit value " + status );
            return status;
        } catch( Exception ex ) {
//            ex.printStackTrace();
            System.err.println("An error has occurred in Execute.");
            return -1;
        }
    }

    private static org.apache.commons.logging.Log log=
        org.apache.commons.logging.LogFactory.getLog( Execute.class );
    
}
