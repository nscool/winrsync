import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RSync {
    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("Usage: RSync -i <identity_file> <remote_user@remote_host:remote_directory> <destination_directory> <num_parallel>");
            System.exit(1);
        }
        int index = 2;
        String identityFile = args[0].contentEquals("-i")? args[1]:null;
        if(identityFile == null)
        	index=0;
        String remoteAddress = args[index];
        // identityFile = args[2];
        String[] remote = remoteAddress.split(":");
        String sourceDir = remote[1];
        String sourceHost = remote[0];
        Path destDir = Paths.get(args[index+1]);
        int numParallel = args.length<5 ? 5 : Integer.parseInt(args[4]);

        if (!Files.isDirectory(destDir)) {
            System.out.println("Destination must be directories.");
            System.exit(1);
        }

        ExecutorService executor = Executors.newFixedThreadPool(numParallel);

        try {
            Process process = Runtime.getRuntime().exec("ssh -i \"" + identityFile + "\" " + sourceHost + " \"find " + sourceDir + " -type f -printf '%T@ %P\\n'\"");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            //while ((line = reader.readLine()) != null) {
            //	System.out.println(line);
            //}
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                double remoteTimestamp = Double.parseDouble(parts[0]);
                Path remoteFile = Paths.get(parts[1]);
                System.out.println("Checking file : " +parts[1] );
                Path localFile = destDir.resolve(remoteFile);

                if (!Files.exists(localFile) || Files.getLastModifiedTime(localFile).toMillis() < (long) (remoteTimestamp * 1000)) {
                    executor.execute(() -> {
                        try {
                            Process scpProcess = Runtime.getRuntime().exec("scp -i \"" + identityFile + "\" -p \"" + sourceHost + ":" + remoteFile + "\" \"" + localFile.toAbsolutePath() +"\"");
                            scpProcess.waitFor();
                            System.out.println("Copied: " + remoteFile + " to " + localFile.toAbsolutePath());
                        } catch (IOException | InterruptedException e) {
                            System.err.println("Failed to copy: " + remoteFile + " to " + destDir);
                            e.printStackTrace();
                        }
                    });
                } else {
                	System.out.println("Ignoring " + remoteFile );
                }
                
            }
            executor.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
