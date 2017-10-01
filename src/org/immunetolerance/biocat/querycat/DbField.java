package org.immunetolerance.biocat.querycat;

/**
 *
 * Created by dwightman on 5/23/2017.
 */
public class DbField
{
    String columnName;
    Object value;

    DbField(String columnName, Object value)
    {
        this.columnName = columnName;
        this.value = value;
    }

    @Override
    public String toString()
    {
        return "|| columnName " + columnName +  " |" + " value='" + value;
    }
}
