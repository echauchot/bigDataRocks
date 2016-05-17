package bigdatarocks.common.dao;

import bigdatarocks.common.bean.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static bigdatarocks.common.constants.Constants.ES_DOCTYPE;
import static bigdatarocks.common.constants.Constants.ES_INDEX;

public class PersonElasticsearchDao extends AElasticsearchDao<Person> implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(PersonElasticsearchDao.class);
    private static final long serialVersionUID = 3208095924979326190L;

    public PersonElasticsearchDao(String nodes, String port, String clusterName) {
        super(nodes, port, clusterName);
    }

    public void index(Person person) throws Exception {
        try {
            String personJson = objectMapper.writeValueAsString(person);
            IndexResponse response =
                    client.prepareIndex(ES_INDEX, ES_DOCTYPE, person.getUserName()).setSource(personJson).get();
        } catch (JsonProcessingException e) {
            LOGGER.error("Error while serializing Person object");
            throw e;
        }

    }

    public Person read(String userName) throws  IOException{
        GetResponse response = client.prepareGet(ES_INDEX, ES_DOCTYPE, userName).get();
        String personJson = response.getSourceAsString();
        try {
            Person person = objectMapper.readValue(personJson, Person.class);
            return person;
        } catch (IOException e) {
            LOGGER.error("Error while deserializing Person document from elasticsearch");
            throw e;
        }
    }

    public List<Person> searchByUserName(String userName) throws Exception {
        List<Person> result = new ArrayList<>();
        MatchQueryBuilder query = QueryBuilders.matchQuery("userName", userName);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(ES_INDEX).setTypes(ES_DOCTYPE).setQuery(query);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        if (response != null) {
            SearchHits hits = response.getHits();
            if (hits != null && hits.getTotalHits() > 0) {
                for (SearchHit hit : hits) {
                    String personJson = hit.getSourceAsString();
                    ObjectMapper objectMapper = new ObjectMapper();
                    try {
                        Person person = objectMapper.readValue(personJson, Person.class);
                        result.add(person);
                    } catch (Exception e) {
                        LOGGER.error("Error while deserializing Elasticsearch Person document");
                        throw e;
                    }
                }
            }

        }
        return result;
    }

    //uses upsert
    public void update(Person person) throws Exception {
        String personJson = null;
        try {
            personJson = objectMapper.writeValueAsString(person);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error while serializing Person object");
            throw e;
        }
        IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_DOCTYPE, person.getUserName()).source(personJson);
        UpdateRequest updateRequest =
                new UpdateRequest(ES_INDEX, ES_DOCTYPE, person.getUserName()).doc(personJson).upsert(indexRequest);
        try {
            client.update(updateRequest).get();
        } catch (Exception e) {
            LOGGER.error("Error in updating Person object");
            throw e;
        }

    }

    public void delete(String userName) {
        DeleteResponse response = client.prepareDelete(ES_INDEX, ES_DOCTYPE, userName).get();
    }


    public long count() {
        long result = 0;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(ES_INDEX).setTypes(ES_DOCTYPE).setSize(0);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        if (response != null){
        }
    return result;
    }

}
