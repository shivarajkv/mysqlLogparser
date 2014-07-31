import java.util.regex.Matcher
import java.util.regex.Pattern

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

        if(ParserHelpers.isContentContainsAProperDmlStatement(content,dml)){
            listOfTableNames.each { tableName ->
                if(content.contains(tableName) && ParserHelpers.doesNotContainIgnoredTableList(content,listOfIgnoredTableNames)){
                    isContentMatched =  true;
                    return;
                }
            }
        }
        return  isContentMatched;
    }


    public static def isContentContainsAProperDmlStatement(String content,String dml){
        boolean isAProperDmlStatement = false;
        /* since there is a chances of table names containing dml statement,
         Wrong logs will be parsed. So matching a regex with appended and prepended spaces will pass that.
          */
        String dmlRegexPrefixedAndSuffixedWithSpace = "[\\s]"+dml+"[\\s]";
        Pattern pattern = Pattern.compile(dmlRegexPrefixedAndSuffixedWithSpace);
        Matcher matcher = pattern.matcher(content);

        if(matcher.find()){
          isAProperDmlStatement = true;
        }
        return isAProperDmlStatement;
    }
}
