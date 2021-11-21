
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author vinicius
 */
public class Main  extends DefaultHandler{

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        Generator gen = new Generator();
        //1 - n queries
        //2 - n tokens
        //3 - query file name
        //4 - base name(directory name)

        String nomeBase = "sigmod";

        //a) Keyword só no conteúdo: 1 a 4 termos, sem labels, só keyword (::k)
        for(int i = 1; i <= 4 ; i++)
            gen.run(12500, 0, i, "1_tcc2", nomeBase);

        //b) Keyword no conteúdo e no label: 5 termos, variando de 0 label a 3 lables ( 5 variações) (l::k)

        gen.run(50000, 0, 5, "2_tcc2", nomeBase);
        gen.run(50000, 1, 5, "3_tcc2", nomeBase);
        gen.run(50000, 2, 5, "4_tcc2", nomeBase);
        gen.run(50000, 3, 5, "5_tcc2", nomeBase);
        //for(int i = 0; i <= 3 ; i++)
        //   gen.run(50000, i, 5, "2_tcc2", "isfdb");

        //c) Key só no conteúdo, com keword de 2, 4 e 6.
        gen.run(50000, 0, 2, "6_tcc2", nomeBase);
        gen.run(50000, 0, 4, "7_tcc2", nomeBase);
        gen.run(50000, 0, 6, "8_tcc2", nomeBase);
        //for(int i = 2; i <= 6 ; i=i+2)
        //gen.run(50000, 0, i, "tcc2", "icde");
    }
    
    
    
}