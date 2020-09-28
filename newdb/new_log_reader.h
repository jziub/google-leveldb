/*
 * new_log_reader.h
 *
 *  Created on: Aug 28, 2020
 *      Author: jiaan
 */

#ifndef LEVELDB_NEWDB_NEW_LOG_READER_H_
#define LEVELDB_NEWDB_NEW_LOG_READER_H_

#include "leveldb/env.h"


namespace leveldb::log {
class NewReader {
  public:
	  NewReader(SequentialFile* file);
	  NewReader(const NewReader&) = delete;
	  NewReader& operator=(const NewReader&) = delete;

	  bool ReadRecord(Slice* record, std::string* scratch);

  private:
	  SequentialFile* const file_;
};

}  // leveldb::log



#endif /* LEVELDB_NEWDB_NEW_LOG_READER_H_ */
