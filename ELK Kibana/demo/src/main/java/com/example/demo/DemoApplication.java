package com.example.demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class DemoApplication {
        public static void main(String[] args) {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("localhost", 9200, "http")));


            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("tesla_employees");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            // Query ra dữ liệu chính xác hoặc tất cả thì chỉ cần thế này
           // searchSourceBuilder.query(QueryBuilders.matchAllQuery());
           // searchSourceBuilder.query(QueryBuilders.matchQuery("name","Mark"));

            // Tính tổng phải cần khai báo
            SumAggregationBuilder aggregationBuilder = AggregationBuilders.sum("Sum_experienceInYears").field("experienceInYears");
            searchSourceBuilder.aggregation(aggregationBuilder);

            // Lưu
            searchRequest.source(searchSourceBuilder);

            Map<String, Object> map=null;
            try {
                SearchResponse searchResponse = null;
                searchResponse =client.search(searchRequest, RequestOptions.DEFAULT);

                // In ra toàn bộ
                if (searchResponse.getHits().getTotalHits().value > 0) {
                    SearchHit[] searchHit = searchResponse.getHits().getHits();
                    for (SearchHit hit : searchHit) {
                        map = hit.getSourceAsMap();
                        System.out.println("map:"+ Arrays.toString(map.entrySet().toArray()));

                    }
                }
                // Tính tổng Sum_experienceInYears
                Aggregations aggregations = searchResponse.getAggregations();
                Sum sum = aggregations.get("Sum_experienceInYears");
                double value = sum.getValue();
                System.out.println("Tổng giá trị của trường 'experienceInYears': " + value);


            } catch (IOException e) {
                e.printStackTrace();
            }

        }
}


