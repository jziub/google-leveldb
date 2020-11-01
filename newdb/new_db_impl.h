/*
 * new_db_impl.h
 *
 *  Created on: Jun 10, 2020
 *      Author: jiaan
 */

#ifndef NEWDB_NEW_DB_IMPL_H_
#define NEWDB_NEW_DB_IMPL_H_

#include <memory>
#include <string>

#include "leveldb/db.h"
#include "newdb/memtable/new_memtable.h"
#include "newdb/new_db_env.h"
#include "newdb/new_log_writer.h"
#include "newdb/new_log_reader.h"
#include "newdb/new_statusor.h"


namespace leveldb {

using namespace ::leveldb::log;

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
    friend class DB;

    std::string dbname_;

		InternalKeyComparator* const internal_key_comparator_;
    NewMemTable* mem_table_;

    NewWriter* wal_writer_;
    NewReader* wal_reader_;
    StatusOr<WritableFile> wal_wrapper_;
    NewDBEnv* const env_;

    Status Recover(const std::string& dbname);
    Status AppendWAL(SequenceNumber seq, ValueType type,
    		const Slice& key, const Slice& value);
    Status LoadWALToMemTable(const std::string& filename);
};

}  // namespace leveldb
#endif /* NEWDB_NEW_DB_IMPL_H_ */
