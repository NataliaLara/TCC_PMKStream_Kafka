package xml.queries.results;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FormatingResults {
    public static void main(String[] args) {
        String absolutPath = "D:\\Computação\\TCC\\Código\\TCC_Final\\TCC_PMKStrem_Kafka\\PMKStream\\src\\xml\\queries\\results\\";
        String base = "isfdb";
        String teste1 = "_original_1_tcc2_0l_[1,2,3,4]t_50000.txt";
        String teste2 = "_original_2_tcc2_0l5t_50000.txt";
        String teste3 = "_original_3_tcc2_1l5t_50000.txt";
        String teste4 = "_original_4_tcc2_2l5t_50000.txt";
        String teste5 = "_original_5_tcc2_3l5t_50000.txt";
        String teste6 = "_original_6_tcc2_0l2t_50000.txt";
        String teste7 = "_original_7_tcc2_0l4t_50000.txt";
        String teste8 = "_original_8_tcc2_0l6t_50000.txt";
        String teste9 = "_original_9_tcc2_0l2t_5000.txt";
        String teste10 = "_original_10_tcc2_0l2t_25000.txt";
        String test = "time_"+base+teste6;
        List<String> results = readingFile(absolutPath+test);
        results.removeIf(value -> value.equals("Initiate") || value.equals("Terminate"));
        lerResultados(results);

    }

    private static void lerResultados(List<String> results){
        for(int i=0; i<results.size();i++){
            String [] query = results.get(i).split(";");
            if(i%40==0){
                System.out.println(query[3]);
            }

            //System.out.println(results.get(i));
            if(query[1].equals("8")){
                System.out.println(query[2]);
            }else {
                System.out.print(query[2]+"\t");
            }
        }
    }

    private static List<String> readingFile(String path){
        List<String> results = new ArrayList<>();
        try {

            FileInputStream stream = new FileInputStream(path);
            InputStreamReader reader = new InputStreamReader(stream);
            BufferedReader br = new BufferedReader(reader);
            String linha = br.readLine();
            while(linha != null) {
                //System.out.println(linha);
                results.add(linha);
                linha = br.readLine();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }
}
