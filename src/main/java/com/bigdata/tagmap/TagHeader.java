package com.bigdata.tagmap;

public class TagHeader extends Delimited
{
    public TagHeader(String[] contents) { super(contents);}
    public TagHeader(String row) { super(row, ","); }

    public String getUserId()    { return getItemAt(0); }
    public String getMovieId()   { return getItemAt(1); }
    public String getTag()       { return getItemAt(2); }
    public String getTimestamp() { return getItemAt(3); }
}
