/**
 * Created by ASUS on 23-Jul-14.
 */
import groovy.sql.Sql

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.util.regex.Matcher
import java.util.regex.Pattern

public class SqlParser{
    private rowSetOfTableNames;
    def sqlLogFile = new File('./files/genquery.log');
    def parserMatchFile = new File('./files/parserMatch.txt');
    boolean isAutoCompletePresentInTheList = false;
    def contentsFromAutoCompleteToCommit = [];
    def DMLStatements = ['select','delete','update','insert'];
    def timeStampUnderConsideration = '';
    def isTimeStampLogged = false;
    def listOfTableNames = [];
    int totalNumberOfLinesReadPreviously = 0;
    def timeStampList =[]

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
        Pattern listOfIgnoredContents = Pattern.compile("show|query");
        contentsFromAutoCompleteToCommit.each { content ->
            Matcher matcher = listOfIgnoredContents.matcher(content);
            if(!matcher.find()){
                DMLStatements.each {dml ->
                    if(ParserHelpers.isContentContainsAnyDMLAndValidTableReference(content,dml,listOfTableNames)){
                        if(!isTimeStampLogged){
                            isTimeStampLogged = true;
                            parserMatchFile.append("\n\n")
                            parserMatchFile.append(timeStampUnderConsideration);
                        }
                        parserMatchFile.append(content+'\n');
                    }
                }
            }

        }
    }


    public  def initiateProcess(){
        addRowSetToList();
        startWatch();
    }

    def startCollectingTheDataForProcessing(){
        int numberOfLinesRead =0;
        sqlLogFile.eachLine { line ->
            line = line.toLowerCase();
            numberOfLinesRead++;
            Matcher timeStampMatcher =timeStampPattern.matcher(line)
            if(numberOfLinesRead > (totalNumberOfLinesReadPreviously + contentsFromAutoCompleteToCommit.size()) ){
                if(timeStampMatcher.find()){
                    if(isAutoCompletePresentInTheList){
                        isAutoCompletePresentInTheList = false;
                        writeDMLStatementsToFile();
                        totalNumberOfLinesReadPreviously = totalNumberOfLinesReadPreviously +contentsFromAutoCompleteToCommit.size();
                        contentsFromAutoCompleteToCommit = [];
                        isTimeStampLogged = false;
                    }else{
                        isAutoCompletePresentInTheList = true;
                        contentsFromAutoCompleteToCommit.add(line);
                        timeStampUnderConsideration = timeStampMatcher.group();+"\n"
                    }
                }else{
                    contentsFromAutoCompleteToCommit.add(line);
                }
            }
        }
        /*sqlLogFile.eachLine { line ->
            numberOfLinesRead = numberOfLinesRead +1;
            if(numberOfLinesRead>contentsFromAutoCompleteToCommit.size() && numberOfLinesRead>totalNumberOfLinesReadPreviously){
                println line;
                line = line.toLowerCase();

                if(timeStampMatcher.find()){
                    if(isAutoCompletePresentInTheList){
                        isAutoCompletePresentInTheList = false;
                        writeDMLStatementsToFile();
                        totalNumberOfLinesReadPreviously = totalNumberOfLinesReadPreviously + contentsFromAutoCompleteToCommit.size();
                        contentsFromAutoCompleteToCommit = [];
                        isTimeStampLogged = false;
                    }
                    isAutoCompletePresentInTheList = true;
                    contentsFromAutoCompleteToCommit.add(line);
                    timeStampUnderConsideration = line+'\n';
                }else{
                    contentsFromAutoCompleteToCommit.add(line);
                }

                println "count"+totalNumberOfLinesReadPreviously
            }
        }*/
    }

    def startWatch(){
        Path myDir = Paths.get("E:/parser/files");
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
            System.out.println("Error: " + e.toString());
        }
        startWatch();
    }

    /*def doesNotContainIgnoredTableList(String content,List listOfIgnoredTableNames){
        boolean doesNotContentHaveIgnoredTable = true;
        listOfIgnoredTableNames.each { ignoredTable ->
            if(content.contains(ignoredTable)){
                doesNotContentHaveIgnoredTable =false;
                return ;
            }
        }
        return doesNotContentHaveIgnoredTable;
    }


    def isContentContainsAnyDMLAndValidTableReference(String content,String dml,listOfTableNames)
    {
        def listOfIgnoredTableNames = ['job_sandbox','webslinger_host_suffix','webslinger_host_mapping','webslinger_server','webslinger_server_base'];
        def isContentMatched = false;
        if(content.contains(dml)){
            listOfTableNames.each { tableName ->
                if(content.contains(tableName) && doesNotContainIgnoredTableList(content,listOfIgnoredTableNames)){
                    isContentMatched =  true;
                    return;
                }
            }
        }
        return  isContentMatched;
    }*/


}







