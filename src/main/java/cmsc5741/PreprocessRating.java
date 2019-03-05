package cmsc5741;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class PreprocessRating {
    public static void main(String[] args) {
        File ratings = new File("src/main/resources/cmsc5741/ratings.csv");
        File noTimeRating = new File("src/main/resources/cmsc5741/noTimeRating.csv");
        //preprocess(ratings,noTimeRating);
        try {
            BufferedReader br = new BufferedReader(new FileReader(noTimeRating));
            String line = "";
            br.readLine();
            ArrayList<String> users = new ArrayList<String>();
            while((line = br.readLine()) != null){
                String[] words = line.split(",");

                if (users.isEmpty()||!users.contains(words[0])){
                    users.add(words[0]);
                }

            }
            System.out.println(users.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private static void preprocess(File ratings, File noTimeRating){
        try {
            BufferedReader br = new BufferedReader(new FileReader(ratings));
            BufferedWriter bw = new BufferedWriter(new FileWriter(noTimeRating));
            String line = "";
            //ignore the first line
            br.readLine();
            while((line = br.readLine()) != null){
                /*
                    line format(content are inside of <>):
                    <userId><,><movieId><,><rating><,><timestamp>
                 */
                String[] words = line.split(",");
                if (words[2] == null){
                    continue;
                }
                float rating = Float.parseFloat(words[2]);
                if (rating >= 0.5 && rating < 2.5){
                    words[2] = "0";
                }else if (rating > 2.5 && rating <= 5.0){
                    words[2] = "1";
                }else{
                    // in case of rating < 0.5 or rating > 5
                    continue;
                }
                bw.write(words[0]+","+words[1]+","+words[2]);
                bw.newLine();
            }
            br.close();
            bw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
