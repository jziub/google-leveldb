/*
 * new_log_reader.cc
 *
 *  Created on: Aug 28, 2020
 *      Author: jiaan
 */

#include "newdb/new_log_reader.h"
#include "newdb/new_log_format.h"
#include "leveldb/env.h"

namespace leveldb::log {

NewReader::NewReader(SequentialFile* file) : file_(file) {

}

bool NewReader::ReadRecord(Slice* record, std::string* scratch) {
	record->clear();
	scratch->clear();

	Slice buffer;

	char header_scratch[kNewHeaderSize];
	Status s = file_->Read(kNewHeaderSize, &buffer, header_scratch);
	if (s.ok()) {
		const char* header = buffer.data();
		const uint32_t low = static_cast<int>(header[0]) & 0xff;
		const uint32_t high = static_cast<int>(header[1]) & 0xff;
		const uint32_t size = low | (high << 8);

		buffer.clear();
		char buffer_scratch[size];
		s = file_->Read(size, &buffer, buffer_scratch);
		if (s.ok()) {
			*record = buffer;
			scratch->assign(buffer.data(), buffer.size());
		}
	}

	return s.ok();
}

}  // leveldb::log

