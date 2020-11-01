/*
 * new_log_rw_test.cc
 *
 *  Created on: Aug 28, 2020
 *      Author: jiaan
 */

#include "gtest/gtest.h"
#include "leveldb/env.h"
#include "util/testutil.h"

#include "newdb/new_log_reader.h"
#include "newdb/new_log_writer.h"
#include "newdb/new_db_env.h"
#include "newdb/new_statusor.h"

namespace leveldb {

namespace {

using namespace ::leveldb::log;

class NewLogReadWriteTest : public testing::Test {
	public:
		NewLogReadWriteTest() : env_(CreateNewDBEnv()) {}

		~NewLogReadWriteTest() {
			delete env_;
		}

	protected:
		NewWriter* writer_;
		NewReader* reader_;
		StatusOr<WritableFile> w_res_;
		StatusOr<SequentialFile> r_res_;

		NewDBEnv* const env_;

		std::string test_full_path_;

		const std::string GetTestFileFullPath(const std::string& file_name) {
			std::string test_dir;
//			ASSERT_LEVELDB_OK(env_->GetTestDirectory(&test_dir));
			env_->GetTestDirectory(&test_dir);
			return test_dir + "/" + file_name;
		}

		void SetUp() override {
			test_full_path_ = GetTestFileFullPath("log-rw.txt");
			w_res_ = env_->NewWritableFile(test_full_path_);
			ASSERT_LEVELDB_OK(w_res_.status());
			writer_ = new NewWriter(w_res_.value());

			r_res_ = env_->NewSequentialFile(test_full_path_);
			ASSERT_LEVELDB_OK(r_res_.status());
			reader_ = new NewReader(r_res_.value());
		}

		void TearDown() override {
			env_->RemoveDir(test_full_path_);
			delete writer_;
			delete reader_;
		}

};

TEST_F(NewLogReadWriteTest, WriteRead) {
	std::string records[5] =
		{"hello world!", "good by", "sing with me", "nike running shoes", "nothing else"};
	for (auto& record : records) {
		ASSERT_LEVELDB_OK(writer_->AddRecord(record));
	}

	Slice data;
	std::string data_scratch;
	for (auto& record : records) {
		EXPECT_TRUE(reader_->ReadRecord(&data, &data_scratch));
		EXPECT_EQ(data_scratch, record);
		EXPECT_EQ(data, Slice(record));
	}
}

}  // namespace

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

