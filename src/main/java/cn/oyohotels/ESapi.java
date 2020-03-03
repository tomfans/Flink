package cn.oyohotels;


import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ESapi {




    public static void main(String[] args) {

        SearchRequestBuilder requestBuilder;

        /**
         * es服务器地址
         */
         TransportClient client = null;


         Long TIME_OUT = 20L;


        requestBuilder = init();



        /**
         * 执行es操作
         * @param requestBuilder
         * @return
         */
        SortBuilder sortBuilder= SortBuilders.fieldSort("data");
        sortBuilder.order(SortOrder.DESC);
        QueryBuilder qb=new MatchAllQueryBuilder();
        SearchResponse searchResponse = requestBuilder.setTimeout(TimeValue.timeValueSeconds(TIME_OUT)).setQuery(qb).setSize(1000).get();
        SearchHits searchHits = searchResponse.getHits();
        for(SearchHit hit:searchHits.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static SearchRequestBuilder init() {
        /**
         * 初始化SearchRequestBuilder
         * @return
         */

         String host = "10.200.20.147";

        /**
         * es服务器端口
         */
         Integer port = 9300;

        /**
         * es集群名称
         */
         String clusterName = "es-for-log";

        /**
         * es索引名称
         */
       //  String esIndex = "test-json-2019.11.27";

         String esIndex = "oyo-prod-json-jar";

        /**
         * es索引下type名称
         */
         String esType = "test-Flink";
         TransportClient client = null;
         SearchRequestBuilder requestBuilder;

        Settings settings = Settings.builder()
                .put("cluster.name", "es-for-log")
                .put("client.transport.sniff", true)
                .put("client.transport.ping_timeout", "600s")
                .build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

            requestBuilder = client.prepareSearch(esIndex).setTypes(esType);
            return requestBuilder;

    }
}
