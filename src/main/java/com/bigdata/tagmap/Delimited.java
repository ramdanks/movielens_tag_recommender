package com.bigdata.tagmap;

public class Delimited
{
    protected final String[] contents;

    public Delimited(String[] contents) { this.contents = contents; }
    public Delimited(String row, String delimiter) { contents = row.split(delimiter); }

    protected String getItemAt(int index)
    {
        if (index < 0 || index >= contents.length)
            return null;
        return contents[index];
    }
}
