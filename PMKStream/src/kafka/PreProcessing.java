package kafka;

import java.io.*;

public class PreProcessing {
    public static void main(String[] args) throws IOException {
        String dirArquivoConsulta = System.getProperty("user.dir")
                + "/src/xml/queries/isfdb"
                +"_formated_1_tcc2_0l_[1,2,3,4]t_50000.txt";
        File f = new File(dirArquivoConsulta);
        BufferedReader in = new BufferedReader(new FileReader(f));
        String line;

        while ((line = in.readLine()) != null) {
            String [] resultadoLinhas = line.split("\\|");
            String nomeTopico = resultadoLinhas[0];
            System.out.println(nomeTopico);
            Admin.createTopics(nomeTopico);
            //Admin.deleteTopics(nomeTopico);
        }
    }
}
