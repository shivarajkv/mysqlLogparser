/**
 * Created by ASUS on 24-Jul-14.
 */
class ParserHelpers {

    public static def doesNotContainIgnoredTableList(String content,List listOfIgnoredTableNames){
        boolean doesNotContentHaveIgnoredTable = true;
        listOfIgnoredTableNames.each { ignoredTable ->
            if(content.contains(ignoredTable)){
                doesNotContentHaveIgnoredTable =false;
                return ;
            }
        }
        return doesNotContentHaveIgnoredTable;
    }


    public static def isContentContainsAnyDMLAndValidTableReference(String content,String dml,listOfTableNames)
    {
        def listOfIgnoredTableNames = ['job_sandbox','webslinger_host_suffix','webslinger_host_mapping','webslinger_server','webslinger_server_base'];
        def isContentMatched = false;
        if(content.contains(dml)){
            listOfTableNames.each { tableName ->
                if(content.contains(tableName) && ParserHelpers.doesNotContainIgnoredTableList(content,listOfIgnoredTableNames)){
                    isContentMatched =  true;
                    return;
                }
            }
        }
        return  isContentMatched;
    }
}
