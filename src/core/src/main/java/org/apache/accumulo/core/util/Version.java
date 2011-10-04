package org.apache.accumulo.core.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Version {
    String package_ = null;
    int major = 0;
    int minor = 0;
    int release = 0;
    String etcetera = null; 
    
    public Version(String everything) {
        parse(everything);
    }
    
    private void parse(String everything) {
        Pattern pattern = Pattern.compile("(([^-]*)-)?(\\d+)(\\.(\\d+)(\\.(\\d+))?)?(-(.*))?");
        Matcher parser = pattern.matcher(everything);
        if (!parser.matches())
            throw new IllegalArgumentException("Unable to parse: " + everything + " as a version");
        
        if (parser.group(1) != null) 
            package_ = parser.group(2);
        major = Integer.valueOf(parser.group(3));
        minor = 0;
        if (parser.group(5) != null)
            minor = Integer.valueOf(parser.group(5));
        if (parser.group(7) != null)
            release = Integer.valueOf(parser.group(7));
        if (parser.group(9) != null)
            etcetera = parser.group(9);


    }
    
    public String getPackage() { return package_;}
    public int getMajorVersion() { return major; }
    public int getMinorVersion() { return minor; }
    public int getReleaseVersion() { return release; }
    public String getEtcetera() { return etcetera; }
    
    
    @Override
	public String toString() {
        StringBuilder result = new StringBuilder();
        if (package_ != null) {
            result.append(package_);
            result.append("-");
        }
        result.append(major);
        result.append(".");
        result.append(minor);
        result.append(".");
        result.append(release);
        if (etcetera != null) {
            result.append("-");
            result.append(etcetera);
        }
        return result.toString();
    }

}
