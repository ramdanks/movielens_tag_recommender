import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Scanner;

/** 
 * This program was meant to reduce the ratings.csv based on minimum tag count of a movie id
 * argument: {tag count minimum} {output file movie tag count mapred} {input path to ratings.csv} {output path to new ratings.csv}
 */
class RatingReducer
{
    public static class MovieTagCountMapred
    {
        String[] contents;

        MovieTagCountMapred(String row) { contents = row.split("\t"); }

        String getMovieId() { return contents[0]; }
        String getTagCount() { return contents[1]; }
    }
    
    public static class RatingHeader
    {
        String[] contents;

        public RatingHeader(String row) { contents = row.split(","); }
    
        public String getUserId()    { return contents[0]; }
        public String getMovieId()   { return contents[1]; }
        public String getRating()    { return contents[2]; }
        public String getTimestamp() { return contents[3]; }
    }

    public static void main(String[] args)
    {
        try
        {
            int tagCountMin = Integer.parseInt(args[0]);
            String tagCountFilepath       = args[1];
            String ratingsFilepath        = args[2];
            String reducedRatingsFilepath = args[3];
            // store movie id uniquely using hashset
            // this set used for lookup table for the next part
            HashSet<String> movieIdSet = new HashSet<>(10000);
            {
                File file = new File(tagCountFilepath);
                Scanner reader = new Scanner(file);
                while (reader.hasNextLine())
                {
                    MovieTagCountMapred mvtc = new MovieTagCountMapred(reader.nextLine());
                    int movieTagCount = Integer.parseInt(mvtc.getTagCount());
                    // if tag count in meets the minimum requested, then register this movie id
                    if (movieTagCount >= tagCountMin)
                        movieIdSet.add(mvtc.getMovieId());
                }
                reader.close();
            }
            // this part is to do selective dropping on the row (entry) which is not
            // meet the requirement tag count. this was done by observing movieIdSet
            {
                File file = new File(ratingsFilepath);
                Scanner reader = new Scanner(file);

                FileWriter fw     = new FileWriter(reducedRatingsFilepath, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out   = new PrintWriter(bw);

                while (reader.hasNextLine())
                {
                    String row = reader.nextLine();
                    RatingHeader header = new RatingHeader(row);
                    if (movieIdSet.contains(header.getMovieId()))
                        out.println(row);
                }

                out.close();
                reader.close();
            }

        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}