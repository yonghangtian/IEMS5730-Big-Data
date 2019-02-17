import java.io.*;

public class Preprocess {
    public static void main(String[] args) {
        File fileM = new File("src/main/resources/hw1-large-dataset/M_large.dat");
        File fileN = new File("src/main/resources/hw1-large-dataset/N_large.dat");
        // can not find file.
        File fileMwithLabel = new File("src/main/resources/hw1-large-dataset/M_large_labeled.dat");
        File fileNwithLabel = new File("src/main/resources/hw1-large-dataset/N_large_labeled.dat");

        /*
            data format:
             matrix M:
                <i> <TAB> <j> <TAB> <mij>
             matrix N:
                <j> <TAB> <k> <TAB> <njk>
         */
        try {
            // edit file M
            BufferedReader br = new BufferedReader(new FileReader(fileM));
            String line = "";
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileMwithLabel));
            int numOfRowsM = 0;
            int numOfJinM = 0;
            while ((line = br.readLine())!=null){
                String[] data = line.split("\t");
                int row = Integer.valueOf(data[0]);
                if (row > numOfRowsM){
                    numOfRowsM = row;
                }
                int j = Integer.valueOf(data[1]);
                if (j > numOfJinM){
                    numOfJinM = j;
                }
                // remember the function of "\t"
                // add label 0 to file M.
                line += "\t"+"0";
                bw.write(line);
                bw.newLine();
            }
            System.out.println("The number of rows in matrix M is "+numOfRowsM);
            System.out.println("The number of J for M is "+numOfJinM);
            br.close();
            bw.close();

            // edit file N
            BufferedReader br1 = new BufferedReader(new FileReader(fileN));
            BufferedWriter bw1 = new BufferedWriter(new FileWriter(fileNwithLabel));
            int numOfColN = 0;
            int numOfJinN = 0;
            while ((line = br1.readLine())!= null){
                String[] data = line.split("\t");
                int col = Integer.valueOf(data[1]);
                if (col > numOfColN){
                    numOfColN = col;
                }
                int j = Integer.valueOf(data[0]);
                if (j > numOfJinN){
                    numOfJinN = j;
                }
                // add label 1 to file N.
                line += "\t"+"1";
                bw1.write(line);
                bw1.newLine();
            }
            System.out.println("The number of cols in matrix N is "+ numOfColN);
            System.out.println("The number of J for N is "+numOfJinN);
            br1.close();
            bw1.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
