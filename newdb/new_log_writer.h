/*
 * new_log_writer.h
 *
 *  Created on: Aug 26, 2020
 *      Author: jiaan
 */

#ifndef LEVELDB_NEWDB_NEW_LOG_WRITER_H_
#define LEVELDB_NEWDB_NEW_LOG_WRITER_H_

#include "leveldb/env.h"


namespace leveldb::log {

class NewWriter {
	public:
		explicit NewWriter(WritableFile* dest);
		NewWriter(const NewWriter&) = delete;
		NewWriter& operator=(const NewWriter&) = delete;

	  Status AddRecord(const Slice& slice);

	private:
		WritableFile* const dest_;
};

}  // namespace leveldb::log

#endif /* LEVELDB_NEWDB_NEW_LOG_WRITER_H_ */
