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


/**
 * Destroys a process running for too long.
 * For example:
 * <pre>
 * ExecuteWatchdog watchdog = new ExecuteWatchdog(30000);
 * Execute exec = new Execute(myloghandler, watchdog);
 * exec.setCommandLine(mycmdline);
 * int exitvalue = exec.execute();
 * if (exitvalue != SUCCESS && watchdog.killedProcess()){
 *              // it was killed on purpose by the watchdog
 * }
 * </pre>
 
 * @author thomas.haas@softwired-inc.com
 * @author <a href="mailto:sbailliez@imediation.com">Stephane Bailliez</a>
 * @see Execute
 */
public class ExecuteWatchdog implements Runnable {
        
    /** the process to execute and watch for duration */
    private Process process;

    /** timeout duration. Once the process running time exceeds this it should be killed */
    private int timeout;

    /** say whether or not the watchog is currently monitoring a process */
    private boolean watch = false;
        
    /** exception that might be thrown during the process execution */
    private Exception caught = null;

    /** say whether or not the process was killed due to running overtime */
    private boolean     killedProcess = false;

    Thread execThread;
    private boolean dontkill=false;
    /**
     * Creates a new watchdog with a given timeout.
     *
     * @param timeout the timeout for the process in milliseconds. It must be greather than 0.
     */
    public ExecuteWatchdog(int timeout) {
        if (timeout < 1) {
            throw new IllegalArgumentException("timeout lesser than 1.");
        }
        this.timeout = timeout;
    }


    public void setDontkill( boolean b ) {
        dontkill=b;
    }
    
    /**
     * Watches the given process and terminates it, if it runs for too long.
     * All information from the previous run are reset.
     * @param process the process to monitor. It cannot be <tt>null</tt>
     * @throws IllegalStateException    thrown if a process is still being monitored.
     */
    public synchronized void start(Process process, Thread execThread) {
        if (process == null) {
            throw new NullPointerException("process is null.");
        }
        if (this.process != null) {
            throw new IllegalStateException("Already running.");
        }
        this.caught = null;
        this.killedProcess = false;
        this.watch = true;
        this.process = process;
        final Thread thread = new Thread(this, "WATCHDOG");
        this.execThread=execThread;
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stops the watcher. It will notify all threads possibly waiting on this object.
     */
    public synchronized void stop() {
        watch = false;
        notifyAll();
    }


    /**
     * Watches the process and terminates it, if it runs for to long.
     */
    public synchronized void run() {
        try {
            // This isn't a Task, don't have a Project object to log.
            // project.log("ExecuteWatchdog: timeout = "+timeout+" msec",  Project.MSG_VERBOSE);
            final long until = System.currentTimeMillis() + timeout;
            long now;
            while (watch && until > (now = System.currentTimeMillis())) {
                try {
                    wait(until - now);
                } catch (InterruptedException e) {}
            }

            // if we are here, either someone stopped the watchdog,
            // we are on timeout and the process must be killed, or
            // we are on timeout and the process has already stopped.
            try {
                // We must check if the process was not stopped
                // before being here
                process.exitValue();
            } catch (IllegalThreadStateException e){
                // the process is not terminated, if this is really
                // a timeout and not a manual stop then kill it.
                //System.out.println("ExecuteWatchdog: timeout");
                if (watch){
                    killedProcess = true;
                    if( ! dontkill ) {
                        //System.out.println("ExecuteWatchdog: destroying process");
                        process.destroy();
                    }
                    if( execThread != null ) {
                        execThread.interrupt();
                    }
                }
            }
        } catch(Exception e) {
            caught = e;
        } finally {
            cleanUp();
        }
        //System.out.println("ExecuteWatchdog: done");
    }
    

    /**
     * reset the monitor flag and the process.
     */
    protected void cleanUp() {
        watch = false;
        process = null;
    }

    /**
     * This method will rethrow the exception that was possibly caught during the
     * run of the process. It will only remains valid once the process has been
     * terminated either by 'error', timeout or manual intervention. Information
     * will be discarded once a new process is ran.
     * @throws  BuildException  a wrapped exception over the one that was silently
     * swallowed and stored during the process run.
     */
//     public void checkException() throws BuildException {
//         if (caught != null) {
//             throw new BuildException("Exception in ExecuteWatchdog.run: "
//                                      + caught.getMessage(), caught);
//         }
//     }

    public Exception getException() {
        return caught;
    }

    /**
     * Indicates whether or not the watchdog is still monitoring the process.
     * @return  <tt>true</tt> if the process is still running, otherwise <tt>false</tt>.
     */
    public boolean isWatching(){
        return watch;
    }

    /**
     * Indicates whether the last process run was killed on timeout or not.
     * @return  <tt>true</tt> if the process was killed otherwise <tt>false</tt>.
     */
    public boolean killedProcess(){
        return killedProcess;
    }
}

