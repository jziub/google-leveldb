/*
 * new_db_env.h
 *
 *  Created on: Sep 5, 2020
 *      Author: jiaan
 */

#ifndef LEVELDB_NEWDB_NEW_DB_ENV_H_
#define LEVELDB_NEWDB_NEW_DB_ENV_H_

#include <memory>

#include "leveldb/env.h"
#include "new_statusor.h"

namespace leveldb {

	class NewDBEnv {
		public:
			StatusOr<WritableFile> NewWritableFile(const std::string& fname);
			StatusOr<SequentialFile> NewSequentialFile(const std::string& fname);
			StatusOr<WritableFile> NewRandomAccessFile(const std::string& fname);
			StatusOr<WritableFile> NewAppendableFile(const std::string& fname);

			Status GetTestDirectory(std::string* path);
			bool FileExists(const std::string& filename);
			Status GetChildren(const std::string& directory_path,
															 std::vector<std::string>* result);
			Status RemoveFile(const std::string& filename);
			Status CreateDir(const std::string& dirname);
			Status RemoveDir(const std::string& dirname);
			Status GetFileSize(const std::string& filename, uint64_t* size);
			Status RenameFile(const std::string& from, const std::string& to);
	};

  Status ReadFileToStringFromNewDBEnv(NewDBEnv* env, const std::string& fname,
                                       std::string* data);

  NewDBEnv* CreateNewDBEnv();
}  //

#endif /* LEVELDB_NEWDB_NEW_DB_ENV_H_ */
