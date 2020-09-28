/*
 * new_mem_table.h
 *
 *  Created on: Sep 27, 2020
 *      Author: jiaan
 */

#ifndef NEWDB_MEMTABLE_NEW_MEMTABLE_H_
#define NEWDB_MEMTABLE_NEW_MEMTABLE_H_

#include "leveldb/db.h"
#include "db/dbformat.h"


namespace leveldb {

class NewMemTable {

	public:
		virtual void Add(SequenceNumber seq, ValueType type, const Slice& key,
						 const Slice& value) = 0;

		virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;

		virtual ~NewMemTable() {};
};

}  // namespace leveldb

#endif /* NEWDB_MEMTABLE_NEW_MEMTABLE_H_ */
