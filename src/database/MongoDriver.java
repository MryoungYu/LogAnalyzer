package database;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDriver {
    private static MongoDriver mongoDriver = new MongoDriver();
    private MongoClient mongoClient = null;
    private MongoDatabase mongoDatabase = null;
    private MongoCollection<Document> mongoCollection = null;
    private MongoDriver(){
        mongoClient = new MongoClient("localhost", 27017);
        mongoDatabase = mongoClient.getDatabase("LogAnalyzer");
    }
    public MongoDriver getInstance() {
        return mongoDriver;
    }
    public MongoDriver use(String collection) {
        if(mongoDatabase == null)
            return null;
        mongoCollection = mongoDatabase.getCollection(collection);
        return mongoDriver;
    }
    public int createCollection(String collection) {
        if(mongoDatabase == null) {
            return 1;
        }
        mongoDatabase.createCollection(collection);
        return 0;
    }
    public int insert(List<Document> documents) {
        if(mongoCollection == null) {
            return 1;
        }
        mongoCollection.insertMany(documents);
        return 0;
    }
    public List<Document> find() {
        FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        List<Document> resList = new ArrayList<>();
        while (mongoCursor.hasNext()) {
            resList.add(mongoCursor.next());
        }
        return resList;
    }
    public List<Document> find(String key, String op, String value) {
        List<Document> resList = new ArrayList<>();
        FindIterable<Document> findIterable = mongoCollection.find(new Document(key, new Document(op, value)));
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            resList.add(mongoCursor.next());
        }
        return resList;
    }

}
