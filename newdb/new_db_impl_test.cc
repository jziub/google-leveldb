#include <unordered_map>

#include "newdb/new_db_impl.h"
#include "leveldb/db.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

namespace {

class NewDBImplTest : public testing::Test {
	public:
		NewDBImplTest() : options_(Options()) {}

		~NewDBImplTest() {}

	protected:
		std::unordered_map<std::string, std::string> kDict = {
				{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}, {"k5", "v5"},
		};

		Options options_;
		DB* db_;

		void SetUp() override {
			ASSERT_LEVELDB_OK(DB::NewOpen(options_, "/tmp/testdb", &db_));
		}

		void TearDown() override {
			DestroyNewDB("/tmp/testdb", options_);
			delete db_;
		}

		void Write() {
			WriteOptions options;
			for (const auto& [key, val] : kDict) {
				ASSERT_LEVELDB_OK(db_->Put(options, key, val));
			}
		}

		void Read() {
			ReadOptions options;
			std::string actual;
			for (const auto& [key, val] : kDict) {
				ASSERT_LEVELDB_OK(db_->Get(options, key, &actual));
				EXPECT_EQ(actual, val);
			}
		}
};

TEST_F(NewDBImplTest, PutGet) {
	Write();
	Read();
}

TEST_F(NewDBImplTest, ReadExisting) {
	Write();

	delete db_;
	ASSERT_LEVELDB_OK(DB::NewOpen(options_, "/tmp/testdb", &db_));

	Read();
}

}  // namespace

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
