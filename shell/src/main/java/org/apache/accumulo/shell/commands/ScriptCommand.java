/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.shell.commands;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class ScriptCommand extends Command {

  // Command to allow user to run scripts, see JSR-223
  // http://www.oracle.com/technetwork/articles/javase/scripting-140262.html

  protected Option list, engine, script, file, args, out, function, object;
  private static final String DEFAULT_ENGINE = "rhino";

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {

    boolean invoke = false;
    ScriptEngineManager mgr = new ScriptEngineManager();

    if (cl.hasOption(list.getOpt())) {
      listJSREngineInfo(mgr, shellState);
    } else if (cl.hasOption(file.getOpt()) || cl.hasOption(script.getOpt())) {
      String engineName = DEFAULT_ENGINE;
      if (cl.hasOption(engine.getOpt())) {
        engineName = cl.getOptionValue(engine.getOpt());
      }
      ScriptEngine engine = mgr.getEngineByName(engineName);
      if (null == engine) {
        shellState.printException(new Exception(engineName + " not found"));
        return 1;
      }

      if (cl.hasOption(object.getOpt()) || cl.hasOption(function.getOpt())) {
        if (!(engine instanceof Invocable)) {
          shellState.printException(new Exception(engineName + " does not support invoking functions or methods"));
          return 1;
        }
        invoke = true;
      }

      ScriptContext ctx = new SimpleScriptContext();

      // Put the following objects into the context so that they
      // are available to the scripts
      // TODO: What else should go in here?
      Bindings b = engine.getBindings(ScriptContext.ENGINE_SCOPE);
      b.put("connection", shellState.getConnector());

      List<Object> argValues = new ArrayList<>();
      if (cl.hasOption(args.getOpt())) {
        String[] argList = cl.getOptionValue(args.getOpt()).split(",");
        for (String arg : argList) {
          String[] parts = arg.split("=");
          if (parts.length == 0) {
            continue;
          } else if (parts.length == 1) {
            b.put(parts[0], null);
            argValues.add(null);
          } else if (parts.length == 2) {
            b.put(parts[0], parts[1]);
            argValues.add(parts[1]);
          }
        }
      }
      ctx.setBindings(b, ScriptContext.ENGINE_SCOPE);
      Object[] argArray = argValues.toArray(new Object[argValues.size()]);

      Writer writer = null;
      if (cl.hasOption(out.getOpt())) {
        File f = new File(cl.getOptionValue(out.getOpt()));
        writer = new FileWriter(f);
        ctx.setWriter(writer);
      }

      if (cl.hasOption(file.getOpt())) {
        File f = new File(cl.getOptionValue(file.getOpt()));
        if (!f.exists()) {
          if (null != writer) {
            writer.close();
          }
          shellState.printException(new Exception(f.getAbsolutePath() + " not found"));
          return 1;
        }
        Reader reader = new FileReader(f);
        try {
          engine.eval(reader, ctx);
          if (invoke) {
            this.invokeFunctionOrMethod(shellState, engine, cl, argArray);
          }
        } catch (ScriptException ex) {
          shellState.printException(ex);
          return 1;
        } finally {
          reader.close();
          if (null != writer) {
            writer.close();
          }
        }
      } else if (cl.hasOption(script.getOpt())) {
        String inlineScript = cl.getOptionValue(script.getOpt());
        try {
          if (engine instanceof Compilable) {
            Compilable compiledEng = (Compilable) engine;
            CompiledScript script = compiledEng.compile(inlineScript);
            script.eval(ctx);
            if (invoke) {
              this.invokeFunctionOrMethod(shellState, engine, cl, argArray);
            }
          } else {
            engine.eval(inlineScript, ctx);
            if (invoke) {
              this.invokeFunctionOrMethod(shellState, engine, cl, argArray);
            }
          }
        } catch (ScriptException ex) {
          shellState.printException(ex);
          return 1;
        } finally {
          if (null != writer) {
            writer.close();
          }
        }
      }
      if (null != writer) {
        writer.close();
      }

    } else {
      printHelp(shellState);
    }
    return 0;
  }

  @Override
  public String description() {
    return "execute JSR-223 scripts";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public String getName() {
    return "script";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    engine = new Option("e", "engine", false, "engine name, defaults to JDK default (Rhino)");
    engine.setArgName("engineName");
    engine.setArgs(1);
    engine.setRequired(false);
    o.addOption(engine);

    OptionGroup inputGroup = new OptionGroup();
    list = new Option("l", "list", false, "list available script engines");
    inputGroup.addOption(list);

    script = new Option("s", "script", true, "use inline script");
    script.setArgName("script text");
    script.setArgs(1);
    script.setRequired(false);
    inputGroup.addOption(script);

    file = new Option("f", "file", true, "use script file");
    file.setArgName("fileName");
    file.setArgs(1);
    file.setRequired(false);

    inputGroup.addOption(file);
    inputGroup.setRequired(true);
    o.addOptionGroup(inputGroup);

    OptionGroup invokeGroup = new OptionGroup();
    object = new Option("obj", "object", true, "name of object");
    object.setArgs(1);
    object.setArgName("objectName:methodName");
    object.setRequired(false);
    invokeGroup.addOption(object);

    function = new Option("fx", "function", true, "invoke a script function");
    function.setArgName("functionName");
    function.setArgs(1);
    function.setRequired(false);
    invokeGroup.addOption(function);
    invokeGroup.setRequired(false);
    o.addOptionGroup(invokeGroup);

    args = new Option("a", "args", true, "comma separated list of key=value arguments");
    args.setArgName("property1=value1,propert2=value2,...");
    args.setArgs(Option.UNLIMITED_VALUES);
    args.setRequired(false);
    o.addOption(args);

    out = new Option("o", "output", true, "output file");
    out.setArgName("fileName");
    out.setArgs(1);
    out.setRequired(false);
    o.addOption(out);

    return o;
  }

  private void listJSREngineInfo(ScriptEngineManager mgr, Shell shellState) throws IOException {
    List<ScriptEngineFactory> factories = mgr.getEngineFactories();
    Set<String> lines = new TreeSet<>();
    for (ScriptEngineFactory factory : factories) {
      lines.add("ScriptEngineFactory Info");
      String engName = factory.getEngineName();
      String engVersion = factory.getEngineVersion();
      String langName = factory.getLanguageName();
      String langVersion = factory.getLanguageVersion();
      lines.add("\tScript Engine: " + engName + " (" + engVersion + ")");
      List<String> engNames = factory.getNames();
      for (String name : engNames) {
        lines.add("\tEngine Alias: " + name);
      }
      lines.add("\tLanguage: " + langName + " (" + langVersion + ")");
    }
    shellState.printLines(lines.iterator(), true);

  }

  private void invokeFunctionOrMethod(Shell shellState, ScriptEngine engine, CommandLine cl, Object[] args) {
    try {
      Invocable inv = (Invocable) engine;
      if (cl.hasOption(function.getOpt())) {
        inv.invokeFunction(cl.getOptionValue(function.getOpt()), args);
      } else if (cl.hasOption(object.getOpt())) {
        String objectMethod = cl.getOptionValue(object.getOpt());
        String[] parts = objectMethod.split(":");
        if (!(parts.length == 2)) {
          shellState.printException(new Exception("Object and Method must be supplied"));
          return;
        }
        String objectName = parts[0];
        String methodName = parts[1];
        Object obj = engine.get(objectName);
        inv.invokeMethod(obj, methodName, args);

      }
    } catch (Exception e) {
      shellState.printException(e);
    }
  }

}
