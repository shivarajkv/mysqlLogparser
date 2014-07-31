/**
 * Created by ASUS on 23-Jul-14.
 */
import java.nio.file.*
import java.util.regex.Matcher
import java.util.regex.Pattern

public class SqlParser{
    private rowSetOfTableNames;
    def sqlLogFile = null;
    def mysqllogDirectory = "C://"
    boolean isAutoCompletePresentInTheList = false;
    def contentsFromAutoCompleteToCommit = [];
    def DMLStatements = ['update','insert','delete'];
    def timeStampUnderConsideration = '';
    def isTimeStampLogged = false;
    def listOfTableNames = [];
    int totalNumberOfLinesReadPreviously = 0;
    def timeStampList =[];

    Pattern timeStampPattern = Pattern.compile("[\\d]{6}[\\s][\\d]{2}:[\\d]{2}:[\\d]{2}");



    SqlParser(rowSetOfTableNames){
        this.rowSetOfTableNames =rowSetOfTableNames;
    }

    def addRowSetToList(){
        rowSetOfTableNames.each { columnNameTableNameMapList ->
            columnNameTableNameMapList.each { columnNameTableNameMap ->
                listOfTableNames.add(columnNameTableNameMap.value)
            }
        }
    }

    def writeDMLStatementsToFile(){
        Pattern listOfIgnoredContents = Pattern.compile("query[\\s]explain");
        contentsFromAutoCompleteToCommit.each { content ->
            Matcher matcher = listOfIgnoredContents.matcher(content);
            if(!matcher.find()){
                DMLStatements.each {dml ->
                    if(ParserHelpers.isContentContainsAnyDMLAndValidTableReference(content,dml,listOfTableNames)){
                        if(!isTimeStampLogged){
                            isTimeStampLogged = true;
                            /*parserMatchFile.append("\n\n")
                            parserMatchFile.append(timeStampUnderConsideration);*/
                            println timeStampUnderConsideration;
                        }
                       /* parserMatchFile.append(content+'\n');*/
                        println content
                    }
                }
            }

        }
    }


    public  def initiateProcess(String mysqllogDirectory,String mysqllogfile){
        this.sqlLogFile = new File(mysqllogfile);
        this.mysqllogDirectory = mysqllogDirectory;
        addRowSetToList();
        startFileReader();
        startWatch();
    }

    void startFileReader() {
        Thread.start {
            println("Begin file read")
            try {
                while (true) {
                    FileInputStream genQueryLog = new FileInputStream(sqlLogFile);
                    genQueryLog.close();
                    Thread.sleep(5000)
                }
            } catch (e) {
                e.printStackTrace();
            }
            println("Done file read")
        }
    }

    def startCollectingTheDataForProcessing(){
        int numberOfLinesRead =0;
        sqlLogFile.eachLine { line ->
            line = line.toLowerCase();
            numberOfLinesRead++;
            Matcher timeStampMatcher =timeStampPattern.matcher(line)
            if(numberOfLinesRead > (totalNumberOfLinesReadPreviously + contentsFromAutoCompleteToCommit.size()) ){
                if(timeStampMatcher.find()){
                    contentsFromAutoCompleteToCommit.add(line);
                    timeStampUnderConsideration = timeStampMatcher.group()+"\n"
                }else{
                    contentsFromAutoCompleteToCommit.add(line);
                    writeDMLStatementsToFile();
                    totalNumberOfLinesReadPreviously = totalNumberOfLinesReadPreviously +contentsFromAutoCompleteToCommit.size();
                    contentsFromAutoCompleteToCommit =[];
                }
            }
        }
    }

    def startWatch(){
        Path myDir = Paths.get(mysqllogDirectory);
        WatchService watcher;
        WatchKey watckKey;
        try {
            watcher = myDir.getFileSystem().newWatchService();
            myDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_MODIFY);

            watckKey = watcher.take();

            List<WatchEvent<?>> events = watckKey.pollEvents();
            for (WatchEvent event : events) {
                if(event.context().toString().equals("genquery.log")){
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE ||event.kind() == StandardWatchEventKinds.ENTRY_MODIFY ) {

                        startCollectingTheDataForProcessing();
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        startWatch();
    }

}







