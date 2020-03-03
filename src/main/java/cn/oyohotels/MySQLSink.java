package cn.oyohotels;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MySQLSink extends ProcessFunction<String, String> {
    private static final long serialVersionUID = 1L;
    private Connection conn;
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

        Statement stmt = conn.createStatement();

        stmt.execute("insert into test (name) values ('" + value +  "')");
        stmt.close();

    }


    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        try{
            conn = DriverManager.getConnection("jdbc:mysql://192.168.71.236:3306/test","admin","S4#PdtY*NBSdcM");

        } catch(Exception ex){
            ex.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {

        conn.close();

    }
}
