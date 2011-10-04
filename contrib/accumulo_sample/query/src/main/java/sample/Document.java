package sample;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
public class Document {
	
	@XmlElement
	private String id = null;
	
	@XmlElement
	private List<Field> field = new ArrayList<Field>();
	
	public Document() {
		super();
	}

	public Document(String id, List<Field> fields) {
		super();
		this.id = id;
		this.field = fields;
	}

	public String getId() {
		return id;
	}

	public List<Field> getFields() {
		return field;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setFields(List<Field> fields) {
		this.field = fields;
	}
	
}
