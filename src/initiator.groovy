/**
 * Created by ASUS on 24-Jul-14.
 */
import groovy.sql.Sql

def props = new Properties()
new File("./resources/db.properties").withInputStream {
    stream -> props.load(stream)
}

String url = props["url"];
String username = props["username"];
String password = props["password"];
def sql = Sql.newInstance( url,username,password, "com.mysql.jdbc.Driver");

def rowSetOfTableNames = sql.rows("show tables");

SqlParser parser = new SqlParser(rowSetOfTableNames);
parser.initiateProcess();
