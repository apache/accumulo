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
package org.apache.accumulo.monitor.servlets;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.util.shell.Shell;

public class ShellServlet extends BasicServlet {
  private static final long serialVersionUID = 1L;
  private Map<String,ShellExecutionThread> userShells = new HashMap<String,ShellExecutionThread>();
  private ExecutorService service = Executors.newCachedThreadPool();

  public static final String CSRF_KEY = "csrf_token";

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Shell";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws IOException {
    HttpSession session = req.getSession(true);
    final String CSRF_TOKEN;
    if (null == session.getAttribute(CSRF_KEY)) {
      // No token, make one
      CSRF_TOKEN = UUID.randomUUID().toString();
      session.setAttribute(CSRF_KEY, CSRF_TOKEN);
    } else {
      // Pull the token out of the session
      CSRF_TOKEN = (String) session.getAttribute(CSRF_KEY);
      if (null == CSRF_TOKEN) {
        throw new RuntimeException("No valid CSRF token exists in session");
      }
    }

    String user = (String) session.getAttribute("user");
    if (user == null) {
      // user attribute is null, check to see if username and password are passed as parameters
      user = req.getParameter("user");
      String pass = req.getParameter("pass");
      String mock = req.getParameter("mock");
      if (user == null || pass == null) {
        // username or password are null, re-authenticate
        sb.append(authenticationForm(req.getRequestURI(), CSRF_TOKEN));
        return;
      }
      try {
        // get a new shell for this user
        ShellExecutionThread shellThread = new ShellExecutionThread(user, pass, mock);
        service.submit(shellThread);
        userShells.put(session.getId(), shellThread);
      } catch (IOException e) {
        // error validating user, reauthenticate
        sb.append("<div id='loginError'>Invalid user/password</div>" + authenticationForm(req.getRequestURI(), CSRF_TOKEN));
        return;
      }
      session.setAttribute("user", user);
    }
    if (!userShells.containsKey(session.getId())) {
      // no existing shell for this user, re-authenticate
      sb.append(authenticationForm(req.getRequestURI(), UUID.randomUUID().toString()));
      return;
    }

    ShellExecutionThread shellThread = userShells.get(session.getId());
    shellThread.getOutput();
    shellThread.printInfo();
    sb.append("<div id='shell'>\n");
    sb.append("<pre id='shellResponse'>").append(shellThread.getOutput()).append("</pre>\n");
    sb.append("<form><span id='shellPrompt'>").append(shellThread.getPrompt());
    sb.append("</span><input type='text' name='cmd' id='cmd' onkeydown='return handleKeyDown(event.keyCode);'>\n");
    sb.append("</form>\n</div>\n");
    sb.append("<script type='text/javascript'>\n");
    sb.append("var url = '").append(req.getRequestURL().toString()).append("';\n");
    sb.append("var xmlhttp = new XMLHttpRequest();\n");
    sb.append("var hsize = 1000;\n");
    sb.append("var hindex = 0;\n");
    sb.append("var history = new Array();\n");
    sb.append("\n");
    sb.append("function handleKeyDown(keyCode) {\n");
    sb.append("  if (keyCode==13) {\n");
    sb.append("    submitCmd(document.getElementById('cmd').value);\n");
    sb.append("    hindex = history.length;\n");
    sb.append("    return false;\n");
    sb.append("  } else if (keyCode==38) {\n");
    sb.append("    hindex = hindex==0 ? history.length : hindex - 1;\n");
    sb.append("    if (hindex == history.length)\n");
    sb.append("      document.getElementById('cmd').value = '';\n");
    sb.append("    else\n");
    sb.append("      document.getElementById('cmd').value = history[hindex];\n");
    sb.append("    return false;\n");
    sb.append("  } else if (keyCode==40) {\n");
    sb.append("    hindex = hindex==history.length ? history.length : hindex + 1;\n");
    sb.append("    if (hindex == history.length)\n");
    sb.append("      document.getElementById('cmd').value = '';\n");
    sb.append("    else\n");
    sb.append("      document.getElementById('cmd').value = history[hindex];\n");
    sb.append("    return false;\n");
    sb.append("  }\n");
    sb.append("  return true;\n");
    sb.append("}\n");
    sb.append("\n");
    sb.append("function submitCmd(cmd) {\n");
    sb.append("  if (cmd=='history') {\n");
    sb.append("    document.getElementById('shellResponse').innerHTML += document.getElementById('shellPrompt').innerHTML+cmd+'\\n';\n");
    sb.append("    document.getElementById('shellResponse').innerHTML += history.join('\\n');\n");
    sb.append("    return\n");
    sb.append("  }\n");
    sb.append("  xmlhttp.open('POST',url+'?cmd='+cmd+'&'+'").append(CSRF_KEY).append("=").append(CSRF_TOKEN).append("',false);\n");
    sb.append("  xmlhttp.send();\n");
    sb.append("  var text = xmlhttp.responseText;\n");
    sb.append("  var index = text.lastIndexOf('\\n');\n");
    sb.append("  if (index >= 0) {\n");
    sb.append("    if (index > 0 && document.getElementById('cmd').type == 'text') {\n");
    sb.append("      if (history.length == hsize)\n");
    sb.append("        history.shift()\n");
    sb.append("      history.push(cmd)\n");
    sb.append("    }\n");
    sb.append("    if (text.charAt(text.length-1)=='*') {\n");
    sb.append("      document.getElementById('cmd').type = 'password';\n");
    sb.append("      text = text.substring(0,xmlhttp.responseText.length-2);\n");
    sb.append("    } else {\n");
    sb.append("      document.getElementById('cmd').type = 'text';\n");
    sb.append("    }\n");
    sb.append("    document.getElementById('shellResponse').innerHTML += text.substring(0,index+1);\n");
    sb.append("    document.getElementById('shellPrompt').innerHTML = text.substring(index+1);\n");
    sb.append("    document.getElementById('cmd').value = '';\n");
    sb.append("    document.getElementById('shell').scrollTop = document.getElementById('cmd').offsetTop;\n");
    sb.append("  } else {\n");
    sb.append("    window.location = url;\n");
    sb.append("  }\n");
    sb.append("}\n");
    sb.append("</script>\n");
    sb.append("<script type='text/javascript'>window.onload = function() { document.getElementById('cmd').select(); }</script>\n");
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    final HttpSession session = req.getSession(true);
    String user = (String) session.getAttribute("user");
    if (user == null || !userShells.containsKey(session.getId())) {
      // no existing shell for user, re-authenticate
      doGet(req, resp);
      return;
    }
    final String CSRF_TOKEN = (String) session.getAttribute(CSRF_KEY);
    if (null == CSRF_TOKEN) {
      // no csrf token, need to re-auth
      doGet(req, resp);
    }
    ShellExecutionThread shellThread = userShells.get(session.getId());
    String cmd = req.getParameter("cmd");
    if (cmd == null) {
      // the command is null, just print prompt
      resp.getWriter().append(shellThread.getPrompt());
      resp.getWriter().flush();
      return;
    }
    shellThread.addInputString(cmd);
    shellThread.waitUntilReady();
    if (shellThread.isDone()) {
      // the command was exit, invalidate session
      userShells.remove(session.getId());
      session.invalidate();
      return;
    }
    // get the shell's output
    StringBuilder sb = new StringBuilder();
    sb.append(shellThread.getOutput().replace("<", "&lt;").replace(">", "&gt;"));
    if (sb.length() == 0 || !(sb.charAt(sb.length() - 1) == '\n'))
      sb.append("\n");
    // check if shell is waiting for input
    if (!shellThread.isWaitingForInput())
      sb.append(shellThread.getPrompt());
    // check if shell is waiting for password input
    if (shellThread.isMasking())
      sb.append("*");
    resp.getWriter().append(sb.toString());
    resp.getWriter().flush();
  }

  private String authenticationForm(String requestURI, String csrfToken) {
    return "<div id='login'><form method=POST action='" + requestURI + "'>"
        + "<table><tr><td>Mock:&nbsp</td><td><input type='checkbox' name='mock' value='mock'></td></tr>"
        + "<tr><td>Username:&nbsp;</td><td><input type='text' name='user'></td></tr>"
        + "<tr><td>Password:&nbsp;</td><td><input type='password' name='pass'></td><td>" + "<input type='hidden' name='" + CSRF_KEY + "' value='" + csrfToken
        + "'/><input type='submit' value='Enter'></td></tr></table></form></div>";
  }

  private static class StringBuilderOutputStream extends OutputStream {
    StringBuilder sb = new StringBuilder();

    @Override
    public void write(int b) throws IOException {
      sb.append((char) (0xff & b));
    }

    public String get() {
      return sb.toString();
    }

    public void clear() {
      sb.setLength(0);
    }
  }

  private static class ShellExecutionThread extends InputStream implements Runnable {
    private Shell shell;
    StringBuilderOutputStream output;
    private String cmd;
    private int cmdIndex;
    private boolean done;
    private boolean readWait;

    private ShellExecutionThread(String username, String password, String mock) throws IOException {
      this.done = false;
      this.cmd = null;
      this.cmdIndex = 0;
      this.readWait = false;
      this.output = new StringBuilderOutputStream();
      ConsoleReader reader = new ConsoleReader(this, output);
      this.shell = new Shell(reader, new PrintWriter(new OutputStreamWriter(output, UTF_8)));
      shell.setLogErrorsToConsole();
      if (mock != null) {
        if (shell.config("--fake", "-u", username, "-p", password))
          throw new IOException("mock shell config error");
      } else if (shell.config("-u", username, "-p", password)) {
        throw new IOException("shell config error");
      }
    }

    @Override
    public synchronized int read() throws IOException {
      if (cmd == null) {
        readWait = true;
        this.notifyAll();
      }
      while (cmd == null) {
        try {
          this.wait();
        } catch (InterruptedException e) {}
      }
      readWait = false;
      int c;
      if (cmdIndex == cmd.length())
        c = '\n';
      else
        c = cmd.charAt(cmdIndex);
      cmdIndex++;
      if (cmdIndex > cmd.length()) {
        cmd = null;
        cmdIndex = 0;
        this.notifyAll();
      }
      return c;
    }

    @Override
    public synchronized void run() {
      Thread.currentThread().setName("shell thread");
      while (!shell.hasExited()) {
        while (cmd == null) {
          try {
            this.wait();
          } catch (InterruptedException e) {}
        }
        String tcmd = cmd;
        cmd = null;
        cmdIndex = 0;
        try {
          shell.execCommand(tcmd, false, true);
        } catch (IOException e) {}
        this.notifyAll();
      }
      done = true;
      this.notifyAll();
    }

    public synchronized void addInputString(String s) {
      if (done)
        throw new IllegalStateException("adding string to exited shell");
      if (cmd == null) {
        cmd = s;
      } else {
        throw new IllegalStateException("adding string to shell not waiting for input");
      }
      this.notifyAll();
    }

    public synchronized void waitUntilReady() {
      while (cmd != null) {
        try {
          this.wait();
        } catch (InterruptedException e) {}
      }
    }

    public synchronized String getOutput() {
      String s = output.get();
      output.clear();
      return s;
    }

    public String getPrompt() {
      return shell.getDefaultPrompt();
    }

    public void printInfo() throws IOException {
      shell.printInfo();
    }

    public boolean isMasking() {
      return shell.isMasking();
    }

    public synchronized boolean isWaitingForInput() {
      return readWait;
    }

    public boolean isDone() {
      return done;
    }
  }
}
