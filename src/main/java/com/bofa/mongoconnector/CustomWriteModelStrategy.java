package com.bofa.mongoconnector;

import java.util.Iterator;
import java.util.Map.Entry;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;

public class CustomWriteModelStrategy implements WriteModelStrategy {

	private static final String ID = null;

	@Override
	public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {
		BsonDocument changeStreamDocument = document.getValueDoc().get();
		BsonDocument fullDocument = changeStreamDocument.getDocument("fullDocument", new BsonDocument());
		if (fullDocument.isEmpty()) {
			return null; // Return null to indicate no op.
		}

		return new ReplaceOneModel<>(Filters.eq(ID, fullDocument.get(ID)), removeEmptyFields(changeStreamDocument));

	}

	/**
	 * Removes empty fields from the given JSON object node.
	 * 
	 * @param an object node
	 * @return the object node with empty fields removed
	 */
	public static BsonDocument removeEmptyFields(final BsonDocument bsonNode) {
		BsonDocument ret = new BsonDocument();
		Iterator<Entry<String, BsonValue>> iter = bsonNode.entrySet().iterator();

		while (iter.hasNext()) {
			Entry<String, BsonValue> entry = iter.next();
			String key = entry.getKey();
			BsonValue value = entry.getValue();

			if (!value.isNull()) {
				if (value.isDocument()) {
					BsonValue removeed = removeEmptyFields(value.asDocument());
					ret.append(key, removeed);
				} else if (value.isArray()) {
					BsonValue removeed = removeEmptyFields(value.asArray());
					ret.append(key, removeed);

				} else if ((value.getBsonType().equals(BsonType.STRING) && !value.asString().getValue().isEmpty())) {
					ret.append(key, value);
				} else if (!value.getBsonType().equals(BsonType.STRING)) {
					ret.append(key, value);
				}
			}
		}

		return ret;
	}

	/**
	 * Removes empty fields from the given JSON array node.
	 * 
	 * @param an array node
	 * @return the array node with empty fields removed
	 */
	public static BsonArray removeEmptyFields(BsonArray array) {
		BsonArray ret = new BsonArray();
		Iterator<BsonValue> iter = array.iterator();

		while (iter.hasNext()) {
			BsonValue value = iter.next();

			if (!value.isNull()) {
				if (value.isArray()) {
					BsonValue removeed = removeEmptyFields(value.asArray());
					ret.add(removeed);
				} else if (value.isDocument()) {
					BsonValue removeed = removeEmptyFields(value.asDocument());
					ret.add(removeed);
				} else if ((value.getBsonType().equals(BsonType.STRING) && !value.asString().getValue().isEmpty())) {
					ret.add(value);
				} else if (!value.getBsonType().equals(BsonType.STRING)) {
					ret.add(value);
				}
			}
		}

		return ret;
	}

}
