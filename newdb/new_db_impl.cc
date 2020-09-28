/*
 * new_db_impl.cc
 *
 *  Created on: Jun 10, 2020
 *      Author: jiaan
 */

#include <newdb/new_db_impl.h>

#include <string>

namespace leveldb {

NewDBImpl::NewDBImpl(const Options& options, const std::string& dbname) {
	// TODO Auto-generated constructor stub

}

NewDBImpl::~NewDBImpl() {
	// TODO Auto-generated destructor stub
}

Status DB::NewOpen(const Options& options, const std::string& dbname,
                   DB** dbptr) {
  *dbptr = new NewDBImpl(options, dbname);
  return Status::OK();
}

Status NewDBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
	db_data_[key.ToString()] = value;
	return Status::OK();
}

Status NewDBImpl::Delete(const WriteOptions& options, const Slice& key) {
	const auto& found = db_data_.find(key.ToString());
	if (found != db_data_.end()) {
		db_data_.erase(found);
		return Status::OK();
	}

	return Status::NotFound(Slice());
}

Status NewDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
	return Status::NotSupported("Batch write is not supported.");
}

Status NewDBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
	const auto& found = db_data_.find(key.ToString());
	if (found != db_data_.end()) {
		value->assign(found->second.data(), found->second.size());
		return Status::OK();
	}

	return Status::NotFound(Slice());
}

Iterator* NewDBImpl::NewIterator(const ReadOptions& options) {
	return nullptr;
}

const Snapshot* NewDBImpl::GetSnapshot() {
	return nullptr;
}

void NewDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {

}

bool NewDBImpl::GetProperty(const Slice& property, std::string* value) {
	return false;
}

void NewDBImpl::GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) {

}

void NewDBImpl::CompactRange(const Slice* begin, const Slice* end) {

}

}  // namespace leveldb



