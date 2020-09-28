/*
 * new_db_impl.h
 *
 *  Created on: Jun 10, 2020
 *      Author: jiaan
 */

#ifndef NEWDB_NEW_DB_IMPL_H_
#define NEWDB_NEW_DB_IMPL_H_

#include <unordered_map>
#include <string>

#include "leveldb/db.h"

namespace leveldb {

class NewDBImpl : public DB {
  public:
		NewDBImpl(const Options& options, const std::string& dbname);
		virtual ~NewDBImpl();

	  Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;

		Status Delete(const WriteOptions& options, const Slice& key) override;

		Status Write(const WriteOptions& options, WriteBatch* updates) override;

		Status Get(const ReadOptions& options, const Slice& key,
		                     std::string* value) override;

		Iterator* NewIterator(const ReadOptions& options) override;

		const Snapshot* GetSnapshot() override;

		void ReleaseSnapshot(const Snapshot* snapshot) override;

		bool GetProperty(const Slice& property, std::string* value) override;

    void GetApproximateSizes(const Range* range, int n,
		                                   uint64_t* sizes) override;

    void CompactRange(const Slice* begin, const Slice* end) override;

  private:
		std::unordered_map<std::string, Slice> db_data_;

//		std::unordered_map<int, int> data_;
};

}  // namespace leveldb
#endif /* NEWDB_NEW_DB_IMPL_H_ */
