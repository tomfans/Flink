package cn.submitjob;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.program.MiniClusterClient;
import java.io.File;


public class submit {

    private static final Logger LOG = LoggerFactory.getLogger(submit.class);

    public static void main(String[] args) throws ProgramInvocationException {

        String jarfile = args[0];

       // Configuration config = GlobalConfiguration.loadConfiguration("/Users/oyo/Documents");

        org.apache.flink.client.cli.CliFrontend.main(new String[]{"run","-m","yarn-cluster","-d", jarfile});
    }
}
