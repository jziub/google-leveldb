/*
 * new_memtable.cc
 *
 *  Created on: Sep 27, 2020
 *      Author: jiaan
 */


#include <list>

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include "leveldb/iterator.h"
#include "newdb/memtable/new_memtable.h"


namespace leveldb {

namespace {
	class NaiveMemTableIterator : public Iterator {
		public:
			NaiveMemTableIterator(const std::list<std::pair<Slice, Slice>>& data)
				: iter_(data.cbegin()), begin_(data.cbegin()), end_(data.cend()) {}

			bool Valid() const override { return iter_ != end_; }

			void Seek(const Slice& key) override {
	//				while (Valid()) {
	//					if (comparator_.Compare(key, kv.internal_key) == 0) {
	//						break;
	//					}
	//				}
			}

			void SeekToFirst() override { iter_ = begin_; }

			void SeekToLast() override {
				iter_ = end_;
				iter_--;
			}

			void Next() override { iter_++; }
			void Prev() override { iter_--; }
			Slice key() const override { return iter_->first; }
			Slice value() const override { return iter_->second; }

			Status status() const override { return Status::OK(); }

		private:
			std::list<std::pair<Slice, Slice>>::const_iterator iter_;
			const std::list<std::pair<Slice, Slice>>::const_iterator begin_;
			const std::list<std::pair<Slice, Slice>>::const_iterator end_;
	};

	ValueType GetValueType(const Slice& internal_key) {
		const char* p = internal_key.data();
		return static_cast<ValueType>(DecodeFixed64(p + internal_key.size() - 8) & 0xff);
	}
}


// allocation has to be done in heap;add a Arena class to promote continuous allocation
void NaiveMemTable::Add(SequenceNumber seq, ValueType type, const Slice& key,
				 const Slice& value) {
	std::pair<Slice, Slice> kv;

	size_t key_size = key.size();
	char* key_buf = new char[key_size + 8];
	std::memcpy(key_buf, key.data(), key_size);
	EncodeFixed64(key_buf+key_size, (seq << 8) | type);
	kv.first = Slice(key_buf, key_size+8);

	size_t value_size = value.size();
	char* value_buf = new char[value_size];
	std::memcpy(value_buf, value.data(), value_size);
	kv.second = Slice(value_buf, value_size);

	// insert sort
	if (data_.empty()) {
		data_.push_back(kv);
		return;
	}

	// a single memtable can only has one sequence number
	std::list<std::pair<Slice, Slice>>::iterator iter;
	for (iter = data_.begin(); iter != data_.end(); iter++) {
		// only compare user_key
		int user_res = comparator_.user_comparator()->Compare(
				ExtractUserKey(kv.first), ExtractUserKey(iter->first));
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
		if (comparator_.Compare(key.internal_key(), kv.first) == 0) {
			switch(GetValueType(kv.first)) {
				case kTypeValue: {
					auto& data = kv.second;
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

Iterator* NaiveMemTable::NewIterator() {
	return new NaiveMemTableIterator(data_);
}

}

