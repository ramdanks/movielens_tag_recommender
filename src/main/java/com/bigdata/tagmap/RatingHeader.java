package com.bigdata.tagmap;

public class RatingHeader extends Delimited
{
    public RatingHeader(String[] contents) { super(contents); }
    public RatingHeader(String row) { super(row, ","); }

    public String getUserId()    { return getItemAt(0); }
    public String getMovieId()   { return getItemAt(1); }
    public String getRating()    { return getItemAt(2); }
    public String getTimestamp() { return getItemAt(3); }
}
