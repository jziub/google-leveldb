/*
 * new_memtable_test.cc
 *
 *  Created on: Sep 28, 2020
 *      Author: jiaan
 */

#include <map>

#include "db/dbformat.h"
#include "include/leveldb/comparator.h"
#include "newdb/memtable/new_memtable.h"
#include "gtest/gtest.h"


namespace leveldb {

namespace {

class NaiveMemTableTest : public testing::Test {
	public:
		NaiveMemTableTest() {
			comparator_ = new InternalKeyComparator(BytewiseComparator());
			memtable_ = new NaiveMemTable(*comparator_);
		}

		~NaiveMemTableTest() {
			delete comparator_;
			delete memtable_;
		}

	protected:
		InternalKeyComparator* comparator_;
		NewMemTable* memtable_;
		const SequenceNumber kSeqNum = 0;
};

TEST_F(NaiveMemTableTest, AddGetValue) {
	std::map<std::string, std::string> kv = {
			{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
	};

	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice(val));
	}

	std::string actual;
	Status actual_status;
	for (const auto& [key, val] : kv) {
		ASSERT_TRUE(memtable_->Get(
				LookupKey(key, kSeqNum),
				&actual, &actual_status));
		EXPECT_EQ(val, actual);
		EXPECT_TRUE(actual_status.ok());
	}
}

TEST_F(NaiveMemTableTest, OverwriteGetValue) {
	std::map<std::string, std::string> kv = {
			{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
	};

	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice(val));
	}

	// overwrite
	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice("new-" + val));
	}

	std::string actual;
	Status actual_status;
	for (const auto& [key, val] : kv) {
		ASSERT_TRUE(memtable_->Get(
				LookupKey(key, kSeqNum),
				&actual, &actual_status));
		EXPECT_EQ("new-" + val, actual);
		EXPECT_TRUE(actual_status.ok());
	}
}

TEST_F(NaiveMemTableTest, DeleteGetValue) {
	std::map<std::string, std::string> kv = {
			{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
	};

	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice(val));
	}

	// delete
	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeDeletion, Slice(key), Slice(""));
	}

	std::string actual;
	Status actual_status;
	for (const auto& [key, val] : kv) {
		ASSERT_FALSE(memtable_->Get(
				LookupKey(key, kSeqNum),
				&actual, &actual_status));
	}
}

TEST_F(NaiveMemTableTest, Iterator) {
	std::map<std::string, std::string> kv = {
			{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
	};

	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice(val));
	}

	auto* iter = memtable_->NewIterator();

	for (const auto& [key, val] : kv) {
		ASSERT_TRUE(iter->Valid());
		EXPECT_TRUE(Slice(key).compare(ExtractUserKey(iter->key())) == 0);
		EXPECT_TRUE(Slice(val).compare(iter->value()) == 0);
		iter->Next();
	}
	ASSERT_FALSE(iter->Valid());
}

TEST_F(NaiveMemTableTest, ReverseIterator) {
	std::map<std::string, std::string> kv = {
			{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
	};

	for (const auto& [key, val] : kv) {
		memtable_->Add(kSeqNum, ValueType::kTypeValue, Slice(key), Slice(val));
	}

	auto* iter = memtable_->NewIterator();
	iter->SeekToLast();
	std::map<std::string, std::string>::reverse_iterator kv_iter;
	for (kv_iter = kv.rbegin(); kv_iter != kv.rend(); kv_iter++) {
		ASSERT_TRUE(iter->Valid());
		EXPECT_TRUE(Slice(kv_iter->first).compare(ExtractUserKey(iter->key())) == 0);
		EXPECT_TRUE(Slice(kv_iter->second).compare(iter->value()) == 0);
		iter->Prev();
	}
	ASSERT_FALSE(iter->Valid());
}

}  // namespace

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
