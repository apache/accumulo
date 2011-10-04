package org.apache.accumulo.core.util.shell;

public class ShellCommandException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public enum ErrorCode
	{
		UNKNOWN_ERROR("Unknown error"),
		UNSUPPORTED_LANGUAGE("Programming language used is not supported"),
		UNRECOGNIZED_COMMAND("Command is not supported"),
		INITIALIZATION_FAILURE("Command could not be initialized"),
		XML_PARSING_ERROR("Failed to parse the XML file");
		
		private String description;

		private ErrorCode(String description) { this.description = description; }
		public String getDescription() { return this.description; }
		public String toString() { return getDescription(); }
	}
	
	private ErrorCode code;
	private String command;
	public ShellCommandException(ErrorCode code) { this(code, null); }
	public ShellCommandException(ErrorCode code, String command) { this.code = code; this.command = command; }
	public String getMessage() { return code + (command != null ? " (" + command + ")" : ""); }
}
