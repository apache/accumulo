package sample;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Results {
	
	@XmlElement
	private List<Document> document = new ArrayList<Document>();

	public Results() {
		super();
	}

	public List<Document> getResults() {
		return document;
	}

	public void setResults(List<Document> results) {
		this.document = results;
	}
	
	public int size() {
		if (null == document)
			return 0;
		else
			return document.size();
	}
	

}
