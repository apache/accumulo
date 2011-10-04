package util;

import org.apache.accumulo.core.data.Key;

public class FieldIndexKeyParser extends KeyParser {

    public static final String DELIMITER = "\0";
    @Override
    public void parse(Key key)
    {
        super.parse (key);

        String[] colFamParts = this.keyFields.get(BaseKeyParser.COLUMN_FAMILY_FIELD).split(DELIMITER);
        this.keyFields.put(FIELDNAME_FIELD, colFamParts.length >= 2 ? colFamParts[1] : "");

        String[] colQualParts = this.keyFields.get(BaseKeyParser.COLUMN_QUALIFIER_FIELD).split(DELIMITER);
        this.keyFields.put(SELECTOR_FIELD, colQualParts.length >= 1 ? colQualParts[0] : "");
        this.keyFields.put(DATATYPE_FIELD, colQualParts.length >= 2 ? colQualParts[1] : "");
        this.keyFields.put(UID_FIELD, colQualParts.length >= 3 ? colQualParts[2] : "");
    }

    @Override
    public BaseKeyParser duplicate ()
    {
        return new FieldIndexKeyParser();
    }

    @Override
    public String getSelector()
    {
        return keyFields.get(SELECTOR_FIELD);
    }

    @Override
    public String getDataType()
    {
        return keyFields.get(DATATYPE_FIELD);
    }

    @Override
    public String getFieldName ()
    {
        return keyFields.get(FIELDNAME_FIELD);
    }

    @Override
    public String getUid()
    {
        return keyFields.get(UID_FIELD);
    }

    public String getDataTypeUid()
    {
        return getDataType()+DELIMITER+getUid();
    }

    // An alias for getSelector
    public String getFieldValue()
    {
        return getSelector();
    }
}
