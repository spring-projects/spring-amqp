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

import java.util.Locale;

/**
 * Condition that tests the OS type.
 *
 * @author <a href="mailto:stefan.bodewig@epost.de">Stefan Bodewig</a>
 * @author <a href="mailto:umagesh@apache.org">Magesh Umasankar</a>
 */
public class Os {
    private static final String osName =
        System.getProperty("os.name").toLowerCase(Locale.US);
    private static final String osArch =
        System.getProperty("os.arch").toLowerCase(Locale.US);
    private static final String osVersion =
        System.getProperty("os.version").toLowerCase(Locale.US);
    private static final String pathSep = System.getProperty("path.separator");

    /**
     * Determines if the OS on which Ant is executing matches the
     * given OS family.
     *
     * @param family The OS family type desired<br />
     *               Possible values:<br />
     *               <ul><li>dos</li>
     *               <li>mac</li>
     *               <li>netware</li>
     *               <li>os/2</li>
     *               <li>unix</li>
     *               <li>windows</li></ul>
     * @since 1.5
     */
    public static boolean isFamily(String family) {
        return isOs(family, null, null, null);
    }

    /**
     * Determines if the OS on which Ant is executing matches the
     * given OS name.
     *
     * @since 1.7
     */
    public static boolean isName(String name) {
        return isOs(null, name, null, null);
    }

    /**
     * Determines if the OS on which Ant is executing matches the
     * given OS architecture.
     *
     * @since 1.7
     */
    public static boolean isArch(String arch) {
        return isOs(null, null, arch, null);
    }

    /**
     * Determines if the OS on which Ant is executing matches the
     * given OS version.
     *
     * @since 1.7
     */
    public static boolean isVersion(String version) {
        return isOs(null, null, null, version);
    }

    /**
     * Determines if the OS on which Ant is executing matches the
     * given OS family, name, architecture and version
     *
     * @param family   The OS family
     * @param name   The OS name
     * @param arch   The OS architecture
     * @param version   The OS version
     *
     * @since 1.7
     */
    public static boolean isOs(String family, String name, String arch,
                               String version) {
        boolean retValue = false;

        if (family != null || name != null || arch != null
            || version != null) {

            boolean isFamily = true;
            boolean isName = true;
            boolean isArch = true;
            boolean isVersion = true;

            if (family != null) {
                if (family.equals("windows")) {
                    isFamily = osName.indexOf("windows") > -1;
                } else if (family.equals("os/2")) {
                    isFamily = osName.indexOf("os/2") > -1;
                } else if (family.equals("netware")) {
                    isFamily = osName.indexOf("netware") > -1;
                } else if (family.equals("dos")) {
                    isFamily = pathSep.equals(";") && !isFamily("netware");
                } else if (family.equals("mac")) {
                    isFamily = osName.indexOf("mac") > -1;
                } else if (family.equals("unix")) {
                    isFamily = pathSep.equals(":")
                        && (!isFamily("mac") || osName.endsWith("x"));
                } else {
                    throw new RuntimeException(
                        "Don\'t know how to detect os family \""
                        + family + "\"");
                }
            }
            if (name != null) {
                isName = name.equals(osName);
            }
            if (arch != null) {
                isArch = arch.equals(osArch);
            }
            if (version != null) {
                isVersion = version.equals(osVersion);
            }
            retValue = isFamily && isName && isArch && isVersion;
        }
        return retValue;
    }
}
