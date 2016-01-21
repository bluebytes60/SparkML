import org.apache.commons.collections.map.HashedMap;

import java.io.*;
import java.util.Map;

/**
 * Created by bluebyte60 on 12/29/15.
 */
public class Preprocess_reuters {

    public static Map<Integer, String> getMap(String line) {
        Map<Integer, String> map = new HashedMap();
        String[] words = line.split(",");
        for (int i = 0; i < words.length; i++) {
            map.put(i, words[i]);
        }
        return map;
    }

    public static void listFilesForFolder(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                System.out.println(fileEntry.getName());
                parse(fileEntry);
            }
        }
    }

    public static void parse(File fileEntry) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileEntry));
            String cat = fileEntry.getName().split("_")[1];
            PrintWriter writer = new PrintWriter(cat + ".parsed", "UTF-8");
            String line = br.readLine();
            Map<Integer, String> map = getMap(line);
            while ((line = br.readLine()) != null) {
                writer.print(cat);
                String[] tokons = line.split(",");
                for (int i = 0; i < tokons.length; i++) {
                    for (int j = 0; j < Integer.parseInt(tokons[i]); j++)
                        writer.print(" " + map.get(i));
                }
                writer.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        listFilesForFolder(new File("data"));
    }
}
