package kafka;

import java.io.*;

public class PreProcessing {
    public static void main(String[] args) throws IOException {
        String dirArquivoConsulta = System.getProperty("user.dir")
                + "/xml/queries/isfdb"
                +"_test_0l5t_50000.txt";
        File f = new File(dirArquivoConsulta);
        BufferedReader in = new BufferedReader(new FileReader(f));
        String line;

        while ((line = in.readLine()) != null) {
            String query=line.toLowerCase().trim()
                    .replaceFirst("::","")
                    .replace("::","-")
                    .replaceAll("\\s+","")
                    .replaceAll("\\+",".");
            System.out.println(query);
            //Admin.createTopics(query);
            Admin.deleteTopics(query);
        }
    }
}
