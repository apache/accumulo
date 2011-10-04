package org.apache.accumulo.core.util.shell;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/*
 * A token is a word in a command in the shell.  The tree that this builds is used for
 * tab-completion of tables, users, commands and certain other parts of the shell that
 * can be realistically and quickly gathered. Tokens can have multiple commands grouped 
 * together and many possible subcommands, although they are stored in a set so duplicates
 * aren't allowed.
 */

public class Token {
	private Set<String> command = new HashSet<String>();
	private Set<Token> subcommands = new HashSet<Token>();
	private boolean caseSensitive = false;
	
	public Token() {}
	
	public Token(String commandName) {
		this();
		command.add(commandName);
	}
	
	public Token(Collection<String> commandNames) {
		this();
		command.addAll(commandNames);
	}
	
	public Token(Set<String> commandNames, Set<Token> subCommandNames) {
		this();
		command.addAll(commandNames);
		subcommands.addAll(subCommandNames);
	}
	
	public void setCaseSensitive(boolean cs) {
		caseSensitive = cs;
	}
	
	public boolean getCaseSensitive() {
		return caseSensitive;
	}
	
	public Set<String> getCommandNames() {
		return command;
	}
	
	public Set<Token> getSubcommandList() {
		return subcommands;
	}

	public Token getSubcommand(String name) {
		Iterator<Token> iter = subcommands.iterator();
		while (iter.hasNext()) {
			Token t = iter.next();
			if (t.containsCommand(name))
				return t;
		}
		return null;
	}
	
	public Set<String> getSubcommandNames() {
		HashSet<String> set = new HashSet<String>();
		for (Token t : subcommands)
			set.addAll(t.getCommandNames());
		return set;
	}
	
	public Set<String> getSubcommandNames(String startsWith) {
		Iterator<Token> iter = subcommands.iterator();
		HashSet<String> set = new HashSet<String>();
		while (iter.hasNext()) {
			Token t = iter.next();
			Set<String> subset = t.getCommandNames();
			for (String s : subset) {
				if (! t.getCaseSensitive()) {
					if (s.toLowerCase().startsWith(startsWith.toLowerCase())) {
						set.add(s);
					}
				}
				else {
					if (s.startsWith(startsWith)) {
						set.add(s);
					}
				}
			}
		}
		return set;
	}

	public boolean containsCommand(String match) {
		Iterator<String> iter = command.iterator();
		while (iter.hasNext()) {
			String t = iter.next();
			if (caseSensitive) {
				if (t.equals(match)) 
					return true;
			}
			else {
				if (t.equalsIgnoreCase(match))
					return true;
			}
		}
		return false;
	}
    public void addSubcommand(Token t) {
		subcommands.add(t);
	}
    public void addSubcommand(Collection<String> t) {
    	for (String a : t) {
    		addSubcommand(new Token(a));
    	}
	}
    public String toString() {
        return this.command.toString();
    }
}
