package ssfsj.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

public class JoinResultsCsvOutput {
    /**
     * Name of the csv file that is going to be generated
     */
    private String fileName;

    public JoinResultsCsvOutput(int boltId){
        this.fileName="fptreejoinbolt_"+boltId+".csv";
    }

    public void createCsvFile(Map<String, Set<String>> joinableDocs){
        //path to folder for storing join results in .csv format
        String pathToFolder = "/tmp/streamJoin/"+this.fileName;
        PrintWriter printWriter = null;
        try {
            File myFile = new File(pathToFolder);
            if(myFile.exists()){
                myFile.delete();
            }
            printWriter = new PrintWriter(myFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DocumentID | JoinableDocuments\n");

        for(String doc : joinableDocs.keySet()){
            String joinableDocsStr = "None";
            if(joinableDocs.get(doc).size() > 0) {
                joinableDocsStr = joinableDocs.get(doc).toString();
                joinableDocsStr = String.join(",", joinableDocsStr.substring(1,joinableDocsStr.length()-1));
            }

            sb.append(doc+" | "+ joinableDocsStr+"\n");
        }
        printWriter.write(sb.toString());
        printWriter.close();

    }
}
