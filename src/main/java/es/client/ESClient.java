package es.client;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


//**坑 5版本支持一个index多个type 6版本支持一个index一个type 7版本会剔除type
public class ESClient {
    private Settings settings;
    private TransportClient client;

    {
        try {
            //配置ES集群名称
            settings = Settings.builder().put("cluster.name", "my-application").build();
            //配置ES连接主节点信息 6版本可以实例化TransportAddress 5版本需要用子类org.elasticsearch.common.transport.InetSocketTransportAddress
//            client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.121"), 9300));
            client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.1.121"), 9300));
        } catch (UnknownHostException e) {
        }
    }
    //判断索引是否存在
    public boolean existsIndex(String index){
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).get();
        return indicesExistsResponse.isExists();
    }

    //创建索引
    public void createIndex(String index) {
        client.admin().indices().prepareCreate(index).get();
    }

    //创建带分片数和副本数的索引
    //创建的分片副本一定不会和主分片在同一节点，所以最大副本数应该是节点数-1
    public void createIndexWithShardsAndReplicas(String index, int shards, int replicas) {
        client.admin().indices().prepareCreate(index).setSettings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
        ).get();
    }

    //查询单个索引
    public void getIndex(String index, String type, String documentId) {
        GetResponse getResponse = client.prepareGet(index, type, documentId).get();
        System.out.println(getResponse.getSourceAsString());
    }

    //查询多个索引
    public void getMultiIndex(String index, String type, String... ids) {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add(index, type, ids).get();
        MultiGetItemResponse[] responses = multiGetItemResponses.getResponses();
        for (MultiGetItemResponse response : responses) System.out.println(response.getResponse().getSourceAsString());
    }

    //删除索引
    public void deleteIndex(String... indices) {
        client.admin().indices().prepareDelete(indices).get();
    }

    //通过构建json方式填充document
    public void createDocument1(String index, String type, String documentId) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("myid", "1");
        map.put("content", "something write in document");
        map.put("myversion", "01");
        String json = JSON.toJSONString(map);
        //直接使用setSource(json)已经弃用，需要指定类型是json
        IndexResponse indexResponse = client.prepareIndex(index, type, documentId).setSource(json, XContentType.JSON).execute().actionGet();
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());
    }

    //通过构建map方式填充document
    public void createDocument2(String index, String type, String documentId) {
        Map<String, String> map = new HashMap<String, String>();
//        map.put("myid", "22");
//        map.put("content", "something write in document");
//        map.put("myversion", "111");
        map.put("id", "22");
        map.put("title", "基于Lucene的搜索服务器");
        map.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
        IndexResponse indexResponse = client.prepareIndex(index, type, documentId).setSource(map).execute().actionGet();
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());
    }

    //通过ES构建器方式填充document
    public void createDocument3(String index, String type, String documentId) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "fromxcontent")
                .field("id", "01")
                .field("value", "created by xcontentfactory.jsonbuilder")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex(index, type, documentId).setSource(xContentBuilder).execute().actionGet();
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());
    }

    //修改单个索引 如果索引不存在则会报错 新增的字段会直接添加，同样的字段则会做修改
    public void updateIndex(String index, String type, String documentId) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest().index(index).type(type).id(documentId)
                .doc(XContentFactory.jsonBuilder().startObject()
                        .field("id", "0222")
                        .field("name", "from updateIndex")
                        .field("info", "extended info")
                        .field("value", "update value").endObject());
        UpdateResponse updateResponse = client.update(updateRequest).actionGet();
        System.out.println("index:" + updateResponse.getIndex());
        System.out.println("type:" + updateResponse.getType());
        System.out.println("id:" + updateResponse.getId());
        System.out.println("version:" + updateResponse.getVersion());
        System.out.println("create:" + updateResponse.getResult());
    }
    //修改或者插入索引
    public void upsertIndex(String index,String type,String documentId) throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(index, type, documentId)
                .source(XContentFactory.jsonBuilder().startObject()
                .field("name","Tom").field("age",18).field("sex","male").endObject());

        UpdateRequest updateRequest = new UpdateRequest(index, type, documentId)
                //存在的索引则修改
                .doc(XContentFactory.jsonBuilder().startObject()
                .field("sex", "female").field("salary", 10000).endObject())
                //不存在的索引则添加 upsert好多个重载，也可以传入IndexRequest
                .upsert(XContentFactory.jsonBuilder().startObject()
                        .field("name","Tom").field("age",18).field("sex","male").endObject());
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println("index:" + updateResponse.getIndex());
        System.out.println("type:" + updateResponse.getType());
        System.out.println("id:" + updateResponse.getId());
        System.out.println("version:" + updateResponse.getVersion());
        System.out.println("create:" + updateResponse.getResult());
    }
    //查询所有索引
    public void matchAllQuery(String index,String type){
        //通过QueryBuilders.选定查询的类型 matchAllQuery查询所有的
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery()).get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
//            System.out.println(hit.getSourceAsMap().get("title"));//可以转换map获取对应field的值
        }
    }
    //按照字符串搜索所有字段的值分词后包含该字符串的
    public void queryStringQuery(String index, String type, String queryString){
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.queryStringQuery(queryString)).get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }
    //通配查询，指定field，按照分词之后的结果搜索
    // * ：表示多个字符（0个或多个字符）
    //？：表示单个字符
    public void wildCardQuery(String index,String type,String field,String word){
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.wildcardQuery(field, word)).get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    public void termQuery(String index,String type,String field,String value){
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.termQuery(field, value)).get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    public void fuzzyQuery(String index,String type,String field,String value){
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.fuzzyQuery(field, value))
                //可以设置分页查询 默认从0条开始，每页显示10条
                //.setFrom(0).setSize(10)
                .get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    //多关键字空格分开检索
    public void matchQuery(String index,String type,String field,String value){
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchQuery(field,value).operator(Operator.AND))
                //可以设置分页查询 默认从0条开始，每页显示10条
                //.setFrom(0).setSize(10)
                .get();
        //获取命中的搜索
        SearchHits hits = searchResponse.getHits();
        //命中的数量
        System.out.println(hits.totalHits);
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }
    }

    public void close() {
        client.close();
    }

    public static void main(String[] args) throws Exception {
        ESClient client = new ESClient();
//        client.createIndex("demo2");
//        client.deleteIndex("demo1");
//        client.createIndexWithShardsAndReplicas("demo3", 8, 2);
//        client.createDocument1("demo1","article","01");
//        client.createDocument2("demo1","article","08");
//        client.createDocument3("demo2","xcontent","01");
//        client.updateIndex("demo1","article","02");
//        client.upsertIndex("demo1","article","05");
//        client.matchAllQuery("demo1","article");
//        client.queryStringQuery("demo1","article","1");
//        client.wildCardQuery("demo1","article","content","*全*");
//        client.termQuery("ikdemo1","iktype","content","全文");
        client.matchQuery("ikdemo1","iktype","content","基于    33");
        client.close();
    }
}
