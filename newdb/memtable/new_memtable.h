/*
 * new_mem_table.h
 *
 *  Created on: Sep 27, 2020
 *      Author: jiaan
 */

#ifndef NEWDB_MEMTABLE_NEW_MEMTABLE_H_
#define NEWDB_MEMTABLE_NEW_MEMTABLE_H_

#include <list>

#include "leveldb/db.h"
#include "db/dbformat.h"


namespace leveldb {

class NewMemTable {

	public:
		virtual void Add(SequenceNumber seq, ValueType type, const Slice& key,
						 const Slice& value) = 0;

		virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;

		virtual Iterator* NewIterator() = 0;

		virtual ~NewMemTable() {};
};

class NaiveMemTable : public NewMemTable {
	public:
		NaiveMemTable(const InternalKeyComparator& comparator)
			: comparator_(comparator) {}

		void Add(SequenceNumber seq, ValueType type, const Slice& key,
						 const Slice& value);
		bool Get(const LookupKey& key, std::string* value, Status* s);
		Iterator* NewIterator();

	private:
//		struct KeyValuePair {
//			Slice internal_key;
//			Slice memtable_value;
//		};
		std::list<std::pair<Slice, Slice>> data_;  // [internal_key, memtable_value]
		InternalKeyComparator comparator_;

		friend class NaiveMemTableIterator;
};

}  // namespace leveldb

#endif /* NEWDB_MEMTABLE_NEW_MEMTABLE_H_ */
