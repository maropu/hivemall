/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.launcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class WorkerProcessBuilder {

    private final List<String> command;

    public WorkerProcessBuilder(
            Class<?> mainClass,  int memoryMb, List<String> arguments, List<String> javaOps) {
        command = new ArrayList<String>();
        command.add("java");
        final String classPath = System.getProperty("java.class.path");
        command.addAll(Arrays.asList("-cp", classPath));
        command.addAll(Arrays.asList("-Xms" + memoryMb + "m", "-Xmx" + memoryMb + "m"));
        if (javaOps != null) command.addAll(javaOps);
        command.add(mainClass.getCanonicalName());
        if (arguments != null) command.addAll(arguments);
    }

    public Process execute() throws IOException {
        Process process = new ProcessBuilder(command).start();
        // TODO: Need to log messages that the forked process outputs
        // via process.getInputStream and process.ErrorStream.
        return process;
    }

    @Override
    public String toString() {
        StringBuilder commandStr = new StringBuilder();
        for (String s : command) {
            commandStr.append(s).append(" ");
        }
        return commandStr.substring(0, commandStr.length() - 1);
    }
}
