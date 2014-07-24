import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        //define a folder root
        Path myDir = Paths.get("E:/parser/files");
        try {
            WatchService watcher = myDir.getFileSystem().newWatchService();
            myDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);

            WatchKey watckKey = watcher.take();

            List<WatchEvent<?>> events = watckKey.pollEvents();
            for (WatchEvent event : events) {
                println event.context().toString();
                if(event.context().equals("genquery.log")){
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE ||event.kind() == StandardWatchEventKinds.ENTRY_MODIFY ) {
                            println event.context();
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}