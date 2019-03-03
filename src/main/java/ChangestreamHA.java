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

public class ChangestreamHA {

	private MongoClient mongoClient;
	private String uuid;
	
	
	public static void main(String[] args) {
		new ChangestreamHA().startWatching();
	}
	
	public ChangestreamHA() {
		this.mongoClient = MongoClients.create("mongodb://localhost:27010,localhost:27021,localhost:27022/?replicaSet=testRS");
		this.uuid = getExistingReplicators();
	}
	
	private void startWatching() {
		BsonTimestamp firstOplogTimestamp = getFirstOplogTimestamp();
		System.out.println(firstOplogTimestamp);
		
		// Double check here, if the oplog timestamp still exists.
		// For this simple example, we add 10 seconds to the first timestamp in the oplog to replay all changes.
		ChangeStreamIterable<Document> csi = mongoClient.watch().startAtOperationTime(new BsonTimestamp(firstOplogTimestamp.getValue() + 10000));
		MongoCursor<ChangeStreamDocument<Document>> iterator = csi.iterator();
		while (iterator.hasNext()) {
			ChangeStreamDocument<Document> doc = iterator.next();
			applyChange(doc);
		}
	}
	
	private void applyChange(ChangeStreamDocument<Document> changeStreamDocument) {
		System.out.println(changeStreamDocument);
		
//		storeReplicationTimestamp(this.uuid, changeStreamDocument.getts)
		
	}
	
	private boolean storeReplicationTimestamp(String uuid, BsonTimestamp ts) {
		
		return true;
	}
	
	/**
	 * Get the replicator entry in the database for this stream.
	 * TODO: Work with application-level partitioning. Here we only have max one.
	 * @return The UUID of the replicator.
	 */
	private String getExistingReplicators() {
		MongoCollection collection = mongoClient.getDatabase("replicators").getCollection("replicators");
		collection.createIndex(new Document("uuid", 1));
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
		
		/*
		 * function () {
        var localdb = this.getSiblingDB("local");

        var result = {};
        var oplog;
        var localCollections = localdb.getCollectionNames();
        if (localCollections.indexOf('oplog.rs') >= 0) {
            oplog = 'oplog.rs';
        } else if (localCollections.indexOf('oplog.$main') >= 0) {
            oplog = 'oplog.$main';
        } else {
            result.errmsg = "neither master/slave nor replica set replication detected";
            return result;
        }

        var ol = localdb.getCollection(oplog);
        var ol_stats = ol.stats();
        if (ol_stats && ol_stats.maxSize) {
            result.logSizeMB = ol_stats.maxSize / (1024 * 1024);
        } else {
            result.errmsg = "Could not get stats for local." + oplog + " collection. " +
                "collstats returned: " + tojson(ol_stats);
            return result;
        }

        result.usedMB = ol_stats.size / (1024 * 1024);
        result.usedMB = Math.ceil(result.usedMB * 100) / 100;

        var firstc = ol.find().sort({$natural: 1}).limit(1);
        var lastc = ol.find().sort({$natural: -1}).limit(1);
        if (!firstc.hasNext() || !lastc.hasNext()) {
            result.errmsg =
                "objects not found in local.oplog.$main -- is this a new and empty db instance?";
            result.oplogMainRowCount = ol.count();
            return result;
        }

        var first = firstc.next();
        var last = lastc.next();
        var tfirst = first.ts;
        var tlast = last.ts;

        if (tfirst && tlast) {
            tfirst = DB.tsToSeconds(tfirst);
            tlast = DB.tsToSeconds(tlast);
            result.timeDiff = tlast - tfirst;
            result.timeDiffHours = Math.round(result.timeDiff / 36) / 100;
            result.tFirst = (new Date(tfirst * 1000)).toString();
            result.tLast = (new Date(tlast * 1000)).toString();
            result.now = Date();
        } else {
            result.errmsg = "ts element not found in oplog objects";
        }

        return result;
    }

		 */
	}
}
