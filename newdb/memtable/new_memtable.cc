/*
 * new_memtable.cc
 *
 *  Created on: Sep 27, 2020
 *      Author: jiaan
 */


#include <list>

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "newdb/memtable/new_memtable.h"

namespace leveldb {

class NaiveMemTable : public NewMemTable {
	public:
		NaiveMemTable(const InternalKeyComparator& comparator)
			: comparator_(comparator) {}

		void Add(SequenceNumber seq, ValueType type, const Slice& key,
						 const Slice& value) {
			KeyValuePair kv(seq, type, key, value);
			data_.push_back(std::move(kv));
		}

		// a linear scan
		bool Get(const LookupKey& key, std::string* value, Status* s) {
			for (const auto& kv : data_) {
				if (comparator_.Compare(key.memtable_key(), kv.memtable_key) == 0) {
					auto& data = kv.memtable_value;
					value->assign(data.data(), data.size());
					*s = Status::OK();
					return true;
				}
			}
			return false;
		}

	private:
		struct KeyValuePair {
			Slice memtable_key;
			Slice memtable_value;

			KeyValuePair(SequenceNumber seq, ValueType type, const Slice& key,
					 const Slice& value) {
				memtable_key = InternalKey(key, seq, type).Encode();
				char* buf = new char[value.size()];
				std::memcpy(buf, value.data(), value.size());
				memtable_value = Slice(buf);  // a deep copy?
			}
		};

		std::list<KeyValuePair> data_;
		InternalKeyComparator comparator_;
};

}

