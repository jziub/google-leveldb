#include <newdb/new_db_impl.h>
#include "leveldb/db.h"

#include "gtest/gtest.h"

namespace leveldb {

namespace {

class NewDBImplTest : public testing::Test {
	public:
		NewDBImplTest() {
			Options options;
			DB::NewOpen(options, "/tmp/testdb", &db_);
		}

		~NewDBImplTest() {
			delete db_;
		}

	protected:
		DB* db_;
};

TEST_F(NewDBImplTest, PutGet) {
	Slice value = "ValueExample";
	Status status;
	status = db_->Put(WriteOptions(), "KeyNameExample", value);
	ASSERT_TRUE(status.ok());

	std::string res;
	status = db_->Get(ReadOptions(), "KeyNameExample", &res);
	ASSERT_TRUE(status.ok());
	EXPECT_EQ(res, "ValueExample");
}

}  // namespace

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
