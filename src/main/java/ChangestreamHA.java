import java.util.UUID;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

// Some Threading classes here: https://docs.aws.amazon.com/streams/latest/dev/kcl2-standard-consumer-java-example.html
// Good Checkpointing: https://docs.aws.amazon.com/streams/latest/dev/kcl2-standard-consumer-python-example.html
public class ChangestreamHA {

	private MongoClient mongoClient;
	private String uuid;
	
	
	public static void main(String[] args) {
		new ChangestreamHA().startWatching();
	}
	
	public ChangestreamHA() {
		this.mongoClient = MongoClients.create("mongodb://localhost:27010,localhost:27021,localhost:27022/?replicaSet=testRS");
		this.uuid = getExistingReplicators();
		startWatching();
	}
	
	private void startWatching() {
		// TODO: 
		// Distinguish the cases:
		// 1) Nothing has been replicated into the target
		//    Copy the dataset into the target database
		//    Find the oplog timestamp of the last operation that has been replicated into the target
		//    Start to listen to changes with this timestamp. The example below is a "hack" to get the first operation in the oplog and adds 10 seconds. This will not work in production and is just an example.
		//
		// 2) We already replicated into the target and need to resume the change stream
		//    Get the resume token of the change process and start the change stream with this resume token, all events after this token will be replayed in the correct order
		
		// The following lines are an example for the first case, but based on the oplog timestamps. This should not be used in production!
		BsonTimestamp firstOplogTimestamp = getFirstOplogTimestamp();
		System.out.println(firstOplogTimestamp);
		
		ChangeStreamIterable<Document> csi = mongoClient.watch().startAtOperationTime(new BsonTimestamp(firstOplogTimestamp.getValue() + 10000));
		MongoCursor<ChangeStreamDocument<Document>> iterator = csi.iterator();
		while (iterator.hasNext()) {
			ChangeStreamDocument<Document> doc = iterator.next();
			applyChange(doc);
		}
		
		// The following is an example to leverage resume tokens
//		BsonDocument resumeToken = getResumeToke(this.uuid);
//		cursor = inventory.watch().resumeAfter(resumeToken).iterator();
//		next = cursor.next();
	}
	
	private void applyChange(ChangeStreamDocument<Document> changeStreamDocument) {
		System.out.println(changeStreamDocument);
		
		// Decide based on the operation, which change needs to be done:
		// Possible events according to: https://docs.mongodb.com/manual/reference/change-events/:
		//		insert Event
		//		update Event
		//		replace Event
		//		delete Event
		//		drop Event
		//		rename Event
		//		dropDatabase Event
		//		invalidate Event
		
		// Store the replication timestamp after a successful operation into the "replicators" collection in order to be resumable
//		storeResumeToken(this.uuid, changeStreamDocument)
		
	}
	
	private boolean storeResumeToken(String uuid, Document changeStreamDocument) {
		
		// TODO: 
		// Here we update the "replicator entry" in the metadata table in order to save the resume token.
		// FIXME: Implementation and error handling
		// Something like: mongoClient.getDatabase("replicators").getCollection("replicators").updateOne(new Document("uuid", this.uuid), new Document("$set", new Document("resumeToken", changeStreamDocument.get("_id").get("data"))));
		
		return true;
	}
	
	/**
	 * Get the replicator entry in the database for this stream.
	 * @return The UUID of the replicator.
	 */
	private String getExistingReplicators() {
		MongoCollection collection = mongoClient.getDatabase("replicators").getCollection("replicators");
		collection.createIndex(new Document("uuid", 1));
		
		// FIXME do not select the first one, but the one for the current collection!!
		FindIterable<Document> replicatorsIterable = collection.find();
		Document replicatorDoc = replicatorsIterable.first();
		if (replicatorDoc != null && replicatorDoc.containsKey("uuid")) {
			return replicatorDoc.getString("uuid");
		}
		else {
			String uuid = UUID.randomUUID().toString();
			System.out.println("Generated new UUID: " + uuid);
			collection.insertOne(new Document("uuid", uuid));
			return uuid;
		}
	}
	
	private BsonTimestamp getFirstOplogTimestamp() {
		
		MongoCollection<Document> oplog = this.mongoClient.getDatabase("local").getCollection("oplog.rs");
		FindIterable<Document> firstIterable = oplog.find().sort(new Document("$natural", 1)).limit(1);
		Document firstDoc = firstIterable.first();
		
		return firstDoc.get("ts", BsonTimestamp.class);
	}
}
