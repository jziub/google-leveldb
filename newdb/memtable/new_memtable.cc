/*
 * new_memtable.cc
 *
 *  Created on: Sep 27, 2020
 *      Author: jiaan
 */


#include <list>
#include <iostream>

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include "newdb/memtable/new_memtable.h"


namespace leveldb {

namespace {
	ValueType GetValueType(const Slice& internal_key) {
		const char* p = internal_key.data();
		return static_cast<ValueType>(DecodeFixed64(p + internal_key.size() - 8) & 0xff);
	}
}

void NaiveMemTable::Add(SequenceNumber seq, ValueType type, const Slice& key,
				 const Slice& value) {
	KeyValuePair kv;

	size_t key_size = key.size();
	char* key_buf = new char[key_size + 8];
	std::memcpy(key_buf, key.data(), key_size);
	EncodeFixed64(key_buf+key_size, (seq << 8) | type);
	kv.internal_key = Slice(key_buf, key_size+8);

	size_t value_size = value.size();
	char* value_buf = new char[value_size];
	std::memcpy(value_buf, value.data(), value_size);
	kv.memtable_value = Slice(value_buf, value_size);

	// insert sort
	if (data_.empty()) {
		data_.push_back(kv);
		return;
	}

	// a single memtable can only has one sequence number
	std::list<KeyValuePair>::iterator iter;
	for (iter = data_.begin(); iter != data_.end(); iter++) {
		// only compare user_key
		int user_res = comparator_.user_comparator()->Compare(
				ExtractUserKey(kv.internal_key), ExtractUserKey(iter->internal_key));
		if (user_res < 0) {
			data_.insert(iter, kv);
			break;
		}

		if (user_res == 0) {
			*iter = kv;
			break;
		}
	}

	if (iter == data_.end()) {
		data_.push_back(kv);
	}
}

// a linear scan
bool NaiveMemTable::Get(const LookupKey& key, std::string* value, Status* s) {
	for (const auto& kv : data_) {
		if (comparator_.Compare(key.internal_key(), kv.internal_key) == 0) {
			switch(GetValueType(kv.internal_key)) {
				case kTypeValue: {
					auto& data = kv.memtable_value;
					value->assign(data.data(), data.size());
					*s = Status::OK();
					return true;
				}

			  case kTypeDeletion: {
			  	*s = Status::NotFound(Slice());
			  	return true;
			  }
			}
		}
	}
	return false;
}
}

