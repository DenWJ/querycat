package org.immunetolerance.biocat.querycat;

/**
 *
 * Created by dwightman on 5/23/2017.
 */
public class DbCol
{
    private String fieldKeyArray;
    String type;
    private Boolean nullable;
    private String caption;

    DbCol(org.json.simple.JSONObject cjo)
    {
        this.fieldKeyArray = (String) ((org.json.simple.JSONArray)cjo.get("fieldKeyArray")).get(0);
        this.type = (String)cjo.get("type");
        this.nullable = (Boolean)cjo.get("nullable");
        this.caption = (String)cjo.get("caption");
    }

    String getSqlType()
    {
        switch(type)
        {
            case "float":
                return "[float]"
                        ;
            case "int":
                return "[int]"
                        ;
            case "string":
                return "[varchar](MAX)"
                        ;
            case "boolean":
                return "[bit]"
                        ;
            case "date":
                return "[datetime]"
                        ;
            default:
                return null;
        }


    }

    @Override
    public String toString()
    {
        return "DbCol{" +
                "fieldKeyArray='" + fieldKeyArray + '\'' +
                ", type='" + type + '\'' +
                ", nullable='" + nullable + '\'' +
                ", caption='" + caption + '\'' +
                '}';
    }
}
