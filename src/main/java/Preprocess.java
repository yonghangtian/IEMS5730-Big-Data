import java.io.*;

public class Preprocess {
    public static void main(String[] args) {
        File fileM = new File("src/main/resources/hw1-small-dataset/M_small.dat");
        File fileN = new File("src/main/resources/hw1-small-dataset/N_small.dat");
        // can not find file.
//        File fileMwithLabel = new File("src/main/resources/hw1-small-dataset/M_small_labeled.dat");
//        File fileNwithLabel = new File("src/main/resources/hw1-small-dataset/N_small_labeled.dat");

        try {
            // edit file M
            BufferedReader br = new BufferedReader(new FileReader(fileM));
            String line = "";
//            BufferedWriter bw = new BufferedWriter(new FileWriter(fileMwithLabel));
            int numOfRowsM = 0;
            while ((line = br.readLine())!=null){
                String[] data = line.split("\t");
                int row = Integer.valueOf(data[0]);
                if (row > numOfRowsM){
                    numOfRowsM = row;
                }
//                // 1    19  2    0
//                //19和2之间的空格数目小于4
//                // \t作用： 输出不足4位时，补齐4位，超出4位且小于8位时，补齐8位。
//                // add label 0 to file M.
//                line += "\t"+"0";
//                bw.write(line);
//                bw.newLine();
            }
            System.out.println("The number of rows in matrix M is "+numOfRowsM);
            br.close();
//            bw.close();

            // edit file N
            BufferedReader br1 = new BufferedReader(new FileReader(fileN));
//            BufferedWriter bw1 = new BufferedWriter(new FileWriter(fileNwithLabel));
            int numOfColN = 0;
            while ((line = br1.readLine())!= null){
                String[] data = line.split("\t");
                int col = Integer.valueOf(data[1]);
                if (col > numOfColN){
                    numOfColN = col;
                }
//                // add label 1 to file N.
//                line += "\t"+"1";
//                bw1.write(line);
//                bw1.newLine();
            }
            System.out.println("The number of cols in matrix N is "+ numOfColN);
            br1.close();
//            bw1.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
