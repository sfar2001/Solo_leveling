import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CSVSplitter {
    public static void main(String[] args) throws IOException {
       String inputFile = "";
        int linesPerFile = 100000; // 100K rows per file
        boolean hasHeader = true;

        splitCSV(inputFile, linesPerFile, hasHeader);
    }

    public static void splitCSV(String inputFile, int linesPerFile, boolean hasHeader) throws IOException {
        File input = new File(inputFile);
        if (!input.exists()) {
            System.err.println("File not found: " + inputFile);
            return;
        }

        String baseName = input.getName().replace(".csv", "");
        String outputDir = input.getParent(); // Get directory of input file

        BufferedReader reader = new BufferedReader(new FileReader(input));
        String header = hasHeader ? reader.readLine() : null;

        List<String> lines = new ArrayList<>();
        String line;
        int fileCount = 0;
        int totalLineCount = 0;

        while ((line = reader.readLine()) != null) {
            lines.add(line);
            totalLineCount++;

            if (lines.size() >= linesPerFile) {
                writeFile(outputDir, baseName, fileCount, header, lines);
                fileCount++;
                lines.clear();

                System.out.println("Processed " + totalLineCount + " lines...");
            }
        }

        // Write remaining lines
        if (!lines.isEmpty()) {
            writeFile(outputDir, baseName, fileCount, header, lines);
            fileCount++;
        }

        reader.close();
        System.out.println("Split into " + fileCount + " files");
        System.out.println("Total lines processed: " + totalLineCount);
    }

    private static void writeFile(String outputDir, String baseName, int fileCount,
                                  String header, List<String> lines) throws IOException {
        String outputFile;
        if (outputDir != null && !outputDir.isEmpty()) {
            outputFile = outputDir + File.separator + baseName + "_part" + fileCount + ".csv";
        } else {
            outputFile = baseName + "_part" + fileCount + ".csv";
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        if (header != null) {
            writer.write(header);
            writer.newLine();
        }

        for (String line : lines) {
            writer.write(line);
            writer.newLine();
        }

        writer.close();
        System.out.println("Created: " + outputFile + " with " + lines.size() + " rows");
    }
}