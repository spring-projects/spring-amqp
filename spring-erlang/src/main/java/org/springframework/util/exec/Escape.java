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

public class Escape {
    private static char[] enlargeArray(char[] in){
        char[] res;

        res = new char[in.length * 2];
        System.arraycopy(in, 0, res, 0, in.length);
        return res;
    }

    /**
     * Escape a string by quoting the magical elements 
     * (such as whitespace, quotes, slashes, etc.)
     */
    public static String escape(String in){
        char[] inChars, outChars, resChars;
        int numOut;

        inChars  = new char[in.length()];
        outChars = new char[inChars.length];
        in.getChars(0, inChars.length, inChars, 0);
        numOut = 0;

        for(int i=0; i<inChars.length; i++){
            if(outChars.length - numOut < 5){
                outChars = enlargeArray(outChars);
            }
            
            if(Character.isWhitespace(inChars[i]) ||
               inChars[i] == '\\' ||
               inChars[i] == '\'' ||
               inChars[i] == '\"' ||
               inChars[i] == '&'  ||
               inChars[i] == ';')
            {
                outChars[numOut++] = '\\';
                outChars[numOut++] = inChars[i];
            } else {
                outChars[numOut++] = inChars[i];
            }
        }

        return new String(outChars, 0, numOut);
    }

    public static void main(String[] args){
        System.out.println(Escape.escape("foo bar"));
        System.out.println(Escape.escape("\\\"foo' bar\""));
    }
}
