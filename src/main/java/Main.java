import com.mongodb.MongoClient;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

public class Main {

    private static String path = "C:\\Users\\Vladimir\\Desktop\\Java\\MongoDemo\\src\\main\\resources\\mongo.csv" ;

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );

        MongoDatabase database = mongoClient.getDatabase("local");

        // Создаем коллекцию
        MongoCollection<Document> collection = database.getCollection("TestSkillDemo");

        // Удалим из нее все документы
        collection.drop();


        Queue<String> students = getStudentsFromFile(path);

        while (!students.isEmpty()){

            String line = students.poll();
            String[] fields = line.split(",",3);

            Queue<String> tempQueue = new LinkedList<>(Arrays.asList(fields));

            while (!tempQueue.isEmpty()){
                collection.insertOne(new Document()
                        .append("name", tempQueue.poll())
                        .append("age", tempQueue.poll())
                        .append("courses", tempQueue.poll()));
            }

        }

        System.out.println("Общее количество студентов в базе " + collection.countDocuments());
        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("Количество студентов старше 40 лет " + collection.countDocuments(query));
        query = BsonDocument.parse("{age : 1}");
        collection.find().sort(query).limit(1).forEach((Consumer<? super Document>) document ->
        {System.out.println("Имя самого молодого студента " + document.get("name"));});


    }
    public static Queue<String> getStudentsFromFile(String path) {

        List<String> lines = null;

        try {
            lines = Files.readAllLines(Paths.get(path));
            //lines.forEach(line -> builder.append(line + "\n"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        Queue<String> students = new LinkedList<>(lines);

        return students;
    }
}