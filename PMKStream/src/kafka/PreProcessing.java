package kafka;

import java.io.*;

public class PreProcessing {
    public static final String topicInputXmlFilesIsfdb = "INPUT_XML_FILES_ISFDB";
    public static final String topicQueriesResultsIsfdb = "OUTPUT_QUERIES_RESULTS_ISFDB";
    public static final String topicInputXmlFilesSigmod = "INPUT_XML_FILES_SIGMOD";
    public static final String topicQueriesResultsSigmod = "OUTPUT_QUERIES_RESULTS_SIGMOD";
    public static final String topicInputXmlFilesIcde = "INPUT_XML_FILES_ICDE";
    public static final String topicQueriesResultsIcde = "OUTPUT_QUERIES_RESULTS_ICDE";
    public static final String topicInputXmlFilesXmark = "INPUT_XML_FILES_XMARK";
    public static final String topicQueriesResultsXmark = "OUTPUT_QUERIES_RESULTS_XMARK";
    public static final String topicQueriesResultTime = "QUERIES_RESULTS_TIME";

    public static final int filesNumberIsfdbBase = 4;
    public static final int filesNumberSigmodBase = 5;
    public static final int filesNumberIcdeBase = 3;
    public static final int filesNumberXmarkBase = 2;

    public static Producer producer = new Producer();

    public static void main(String[] args) throws IOException {

        //preProcessingDeleteRequiredTopics();
        preProcessingCreateRequiredTopics();
        preProcessingProduceXmlFilesInTopics();
        //preProcessingQueries();

    }

    private static void preProcessingProduceXmlFilesInTopics(){
        String baseNameIsfdb = "isfdb";
        producingMessagensInTopics(baseNameIsfdb, filesNumberIsfdbBase, topicInputXmlFilesIsfdb);

        String baseNameSigmog = "sigmod";
        producingMessagensInTopics(baseNameSigmog, filesNumberSigmodBase, topicInputXmlFilesSigmod);

        String baseNameIcde = "icde";
        producingMessagensInTopics(baseNameIcde, filesNumberIcdeBase, topicInputXmlFilesIcde);

        String baseNameXmark = "xmark";
        producingMessagensInTopics(baseNameXmark, filesNumberXmarkBase, topicInputXmlFilesXmark);

        System.out.println("Arquivos XML enviados com sucesso");

    }

    private static void producingMessagensInTopics(String baseFilesName, int nXmlFiles, String topicName){
        String rootPath = System.getProperty("user.dir")
                + "/src/xml/datasets/"+baseFilesName+"/";
        try{
            for(int i=1; i<=nXmlFiles;i++){
                producer.produce(topicName,(rootPath+i));
                //System.out.println(topicName+"\t"+rootPath+i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static void preProcessingDeleteRequiredTopics(){
        try {
            Admin.deleteTopics(topicInputXmlFilesIsfdb);
            Admin.deleteTopics(topicQueriesResultsIsfdb);
            Admin.deleteTopics(topicInputXmlFilesSigmod);
            Admin.deleteTopics(topicQueriesResultsSigmod);
            Admin.deleteTopics(topicInputXmlFilesIcde);
            Admin.deleteTopics(topicQueriesResultsIcde);
            Admin.deleteTopics(topicInputXmlFilesXmark);
            Admin.deleteTopics(topicQueriesResultsXmark);
            Admin.deleteTopics(topicQueriesResultTime);
            System.out.println("Topicos deletados com sucesso");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void preProcessingCreateRequiredTopics(){
        try {
            Admin.createTopics(topicInputXmlFilesIsfdb);
            Admin.createTopics(topicQueriesResultsIsfdb);
            Admin.createTopics(topicInputXmlFilesSigmod);
            Admin.createTopics(topicQueriesResultsSigmod);
            Admin.createTopics(topicInputXmlFilesIcde);
            Admin.createTopics(topicQueriesResultsIcde);
            Admin.createTopics(topicInputXmlFilesXmark);
            Admin.createTopics(topicQueriesResultsXmark);
            Admin.createTopics(topicQueriesResultTime);
            System.out.println("Topicos criados com sucesso");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static  void preProcessingQueries(){
        String dirArquivoConsulta = System.getProperty("user.dir")
                + "/src/xml/queries/isfdb"
                +"_formated_1_tcc2_0l_[1,2,3,4]t_50000.txt";
        File f = new File(dirArquivoConsulta);
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(f));
            String line;

            while ((line = in.readLine()) != null) {
                String [] resultadoLinhas = line.split("\\|");
                String nomeTopico = resultadoLinhas[0];
                System.out.println(nomeTopico);
                //Admin.createTopics(nomeTopico);
                Admin.deleteTopics(nomeTopico);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
