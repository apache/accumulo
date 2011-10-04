package org.apache.accumulo.server.upgrade;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


/**
 * Perform an upgrade of the existing conf/accumulo-site.xml files
 */
public class UpgradeAccumuloSite {
    
    private static class PropertyValueDescription {
        String property = "";
        String value = "";
        String description = "";
        PropertyValueDescription(String p, String v, String d) {
            property = p;
            value = v;
            description = d;
        }
    }
    
    private static class ConfigHandler extends DefaultHandler {
        String property = "";
        String value = "";
        String description = "";
        StringBuilder more = null;
        List<PropertyValueDescription> contents = new ArrayList<PropertyValueDescription>(); 
        
        
        @Override
        public void startElement(String uri, String localName, String name,
                                 Attributes attributes) throws SAXException {
            more = new StringBuilder();
        }
        
        @Override
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            more.append(ch, start, length);
        }
       
        @Override
        public void endElement(String uri, String localName, String name)
                throws SAXException {
            String s = more.toString().trim();
            if (name.equals("name")) {
                property = s;
            } else if (name.equals("value")) {
                value = s;
            } else if (name.equals("description")) {
                description = s;
            } else if (name.equals("property")) {
                contents.add(new PropertyValueDescription(property, value, description));
                property = value = description = "";
            }
        }
    }
    
    
    public static void main(String args[]) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();
        ConfigHandler c = new ConfigHandler();
        parser.parse(new InputSource(System.in), c);
        PrintStream out = System.out;
        out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
        out.println("\n<configuration>\n");
        
        for (PropertyValueDescription entry : c.contents) {
                
            Property p = Property.getPropertyByKey(entry.property);
            if (p != null) {
                if (p.getDefaultValue().equals(entry.value)) continue;

                if (p.getType().equals(PropertyType.TIMEDURATION)) {
                    int value = Integer.parseInt(entry.value);
                    if (value > 1000) {
                        if (value % 1000 == 0)
                            entry.value = String.format("%ds", value / 1000);
                        else
                            entry.value = String.format("%.2fs", value / 1000.);
                    }
                }
                if (entry.property.equals("table.scan.max.time"))
                    entry.value = entry.value + "ms";
            }
            
            out.println("   <property>");
            out.println("      <name>" + entry.property + "</name>");
            
            out.println("      <value>" + entry.value + "</value>");
            if (entry.description.trim().length() > 0)
                out.println("      <description>" + entry.description + "</description>");
            out.println("   </property>\n");
        }
        out.println("</configuration>");
    }

}
