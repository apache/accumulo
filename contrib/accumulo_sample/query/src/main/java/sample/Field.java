package sample;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

@XmlAccessorType(XmlAccessType.FIELD)
public class Field {
	
	@XmlAttribute
	private String name = null;
	@XmlValue
	private String value = null;
	
	public Field() {
		super();
	}

	public Field(String fieldName, String fieldValue) {
		super();
		this.name = fieldName;
		this.value = fieldValue;
	}

	public String getFieldName() {
		return name;
	}

	public String getFieldValue() {
		return value;
	}

	public void setFieldName(String fieldName) {
		this.name = fieldName;
	}

	public void setFieldValue(String fieldValue) {
		this.value = fieldValue;
	}
	
}
