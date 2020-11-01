/*
 * new_db_impl.cc
 *
 *  Created on: Jun 10, 2020
 *      Author: jiaan
 */

#include <string>
#include <iostream>

#include "db/dbformat.h"
#include "newdb/new_db_impl.h"
#include "newdb/memtable/new_memtable.h"
#include "newdb/new_db_env.h"
#include "newdb/new_log_reader.h"
#include "newdb/new_log_writer.h"
#include "newdb/new_statusor.h"


namespace leveldb {

using namespace ::leveldb::log;

namespace {
	const SequenceNumber kSeqNum = 0;
	const int kHeaderSize = 4;
	const int kKeySuffixSize = 8;

	const std::string GetWALPath(const std::string& dbname) {
		return dbname + "/wal.log";
	}
}

NewDBImpl::NewDBImpl(const Options& options, const std::string& dbname)
		: internal_key_comparator_(new InternalKeyComparator(BytewiseComparator())),
			dbname_(dbname),
			mem_table_(nullptr),
			env_(options.new_env),
			wal_writer_(nullptr),
			wal_wrapper_(StatusOr<WritableFile>()) {
}

NewDBImpl::~NewDBImpl() {}

Status DB::NewOpen(const Options& options, const std::string& dbname,
                   DB** dbptr) {
  auto* db_impl = new NewDBImpl(options, dbname);
  Status s = db_impl->Recover(dbname);
  if (s.ok()) {
  	*dbptr = db_impl;
  }

  return s;

}

Status NewDBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
	Status s = AppendWAL(kSeqNum, ValueType::kTypeValue, key, value);
	if (!s.ok()) {
		return s;
	}

	mem_table_->Add(kSeqNum, ValueType::kTypeValue, key, value);
	return Status::OK();
}

Status NewDBImpl::Delete(const WriteOptions& options, const Slice& key) {
	Slice dummy;
	AppendWAL(kSeqNum, ValueType::kTypeValue, key, dummy);
	mem_table_->Add(kSeqNum, ValueType::kTypeDeletion, key, dummy);
	return Status::NotFound(Slice());
}

Status NewDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
	return Status::NotSupported("Batch write is not supported.");
}

Status NewDBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
	Status s;
	if (mem_table_->Get(LookupKey(key, kSeqNum), value, &s)) {
		return s;
	}
	return Status::IOError(Slice());
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

Status NewDBImpl::Recover(const std::string& dbname) {
	mem_table_ = new NaiveMemTable(*internal_key_comparator_);

	const std::string log_path = GetWALPath(dbname);

	if (env_->FileExists(log_path)) {
		Status s = LoadWALToMemTable(log_path);
		if (!s.ok()) {
			return s;
		}
		wal_wrapper_ = env_->NewAppendableFile(log_path);
	} else {
		env_->CreateDir(dbname);
		wal_wrapper_ = env_->NewWritableFile(log_path);
	}

	Status s = wal_wrapper_.status();
	if (s.ok()) {
		wal_writer_ = new NewWriter(wal_wrapper_.value());
	}
	return s;
}

/**
 * int + key_data + 8 + value_data
 */
Status NewDBImpl::AppendWAL(SequenceNumber seq, ValueType type,
		const Slice& key, const Slice& value) {
	size_t key_size = key.size();
	size_t value_size = value.size();

	size_t buf_size = kHeaderSize + key_size + kKeySuffixSize + value_size;
	char buf[buf_size];
	EncodeFixed32(buf, key_size + kKeySuffixSize);

	std::memcpy(kHeaderSize + buf, key.data(), key_size);
	EncodeFixed64(kHeaderSize + buf + key_size, (seq << 8) | type);
	std::memcpy(kHeaderSize + buf + key_size + kKeySuffixSize, value.data(), value_size);

	return wal_writer_->AddRecord(Slice (buf, buf_size));
}

Status NewDBImpl::LoadWALToMemTable(const std::string& filename) {
	StatusOr<SequentialFile> seqfile_wrapper = env_->NewSequentialFile(filename);
	Status s = seqfile_wrapper.status();
	if (s.ok()) {
		Slice data;
		std::string data_scratch;
		wal_reader_ = new NewReader(seqfile_wrapper.value());
		while(wal_reader_->ReadRecord(&data, &data_scratch)) {
			const auto* ptr = data.data();
			uint32_t internal_key_size = DecodeFixed32(ptr);
			Slice key(ptr + kHeaderSize, internal_key_size - kKeySuffixSize);
			uint64_t key_suffix = DecodeFixed64(ptr + kHeaderSize + internal_key_size - kKeySuffixSize);
			ValueType type = static_cast<ValueType>(0xff & key_suffix);
			Slice value(ptr + kHeaderSize + internal_key_size);

			SequenceNumber seq = (0xff00 & key_suffix);
			mem_table_->Add(seq, type, key, value);
		}
	}

	return s;
}

Status DestroyNewDB(const std::string& dbname, const Options& options) {
	auto* new_env = options.new_env;
	std::vector<std::string> filenames;
	Status s = new_env->GetChildren(dbname, &filenames);
	if (s.ok()) {
		for (const auto& filename : filenames) {
			if (filename == "wal.log") {
				s = new_env->RemoveFile(dbname + "/" + filename);
				if (!s.ok()) {
					return s;  // DB is in inconsistent state, don't delete others
				}
			}
		}
	}

	new_env->RemoveDir(dbname);

	return Status::OK();  // ignore directory not found error
}

}  // namespace leveldb



