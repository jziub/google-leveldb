/*
 * new_env_posix_test.cc
 *
 *  Created on: Jul 17, 2020
 *      Author: jiaan
 */


#include "leveldb/env.h"
#include "newdb/new_db_env.h"
#include "util/testutil.h"

#include "gtest/gtest.h"


namespace leveldb {

namespace {

class NewEnvTest : public testing::Test {
	public:
		NewEnvTest() : db_env_(CreateNewDBEnv()) {}

		~NewEnvTest() {
			delete db_env_;
		}

	protected:
		NewDBEnv* db_env_;
		std::string test_full_path_db_env_;

		void SetUp() {
			test_full_path_db_env_ = GetNewDBEnvTestFileFullPath("db_env_file.txt");
		}

		void TearDown() {
			db_env_->RemoveDir(test_full_path_db_env_);
		}

		const std::string GetNewDBEnvTestFileFullPath(const std::string& file_name) {
			std::string test_dir;
			db_env_->GetTestDirectory(&test_dir);
			return test_dir + "/" + file_name;
		}
};

TEST_F(NewEnvTest, ReadWrite) {
	Random rnd(test::RandomSeed());
	const size_t kDataSize = 10 * 1048576;

	// write
	const auto& w_res = db_env_->NewWritableFile(test_full_path_db_env_);
	ASSERT_LEVELDB_OK(w_res.status());
	auto* writable_file = w_res.value();
	std::string data;
	while (data.size() < kDataSize) {
		int len = rnd.Skewed(18);  // Up to 2^18 - 1, but typically much smaller
		std::string r;
		test::RandomString(&rnd, len, &r);
		ASSERT_LEVELDB_OK(writable_file->Append(r));
		data += r;
		if (rnd.OneIn(10)) {
			ASSERT_LEVELDB_OK(writable_file->Flush());
		}
	}
	ASSERT_LEVELDB_OK(writable_file->Sync());
	ASSERT_LEVELDB_OK(writable_file->Close());

	// read
	const auto& r_res = db_env_->NewSequentialFile(test_full_path_db_env_);
	ASSERT_LEVELDB_OK(r_res.status());
	auto* sequential_file = r_res.value();
	std::string read_data;
	while (read_data.size() < data.size()) {
		int len = rnd.Skewed(18);
		char s[len];
		Slice r;
		ASSERT_LEVELDB_OK(sequential_file->Read(len, &r, s));
		read_data.append(r.data(), r.size());
	}
	ASSERT_EQ(read_data, data);
}

TEST_F(NewEnvTest, Append) {
	// write
	const auto& w_res = db_env_->NewWritableFile(test_full_path_db_env_);
	ASSERT_LEVELDB_OK(w_res.status());
	auto* writable_file = w_res.value();
	ASSERT_LEVELDB_OK(writable_file->Append("hello world!"));
	ASSERT_LEVELDB_OK(writable_file->Close());

	// append
	const auto& a_res = db_env_->NewAppendableFile(test_full_path_db_env_);
	ASSERT_LEVELDB_OK(a_res.status());
	auto* appendable_file = a_res.value();
	ASSERT_LEVELDB_OK(appendable_file->Append("hello another world!"));
	ASSERT_LEVELDB_OK(appendable_file->Close());

	// read
	std::string data;
	ASSERT_LEVELDB_OK(ReadFileToStringFromNewDBEnv(db_env_, test_full_path_db_env_, &data));
	EXPECT_EQ(data, "hello world!hello another world!");
}

}  // namespace

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


