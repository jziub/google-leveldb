/*
 * new_log_writer.cc
 *
 *  Created on: Aug 26, 2020
 *      Author: jiaan
 */

#include "newdb/new_log_writer.h"
#include "newdb/new_log_format.h"
#include "leveldb/env.h"


namespace leveldb::log {

NewWriter::NewWriter(WritableFile* dest): dest_(dest) {

}

Status NewWriter::AddRecord(const Slice& slice) {
	const auto* ptr = slice.data();
	size_t size = slice.size();

	char header[kNewHeaderSize];
	header[0] = static_cast<char>(size & 0xff);  // low 8 bits
	header[1] = static_cast<char>(size >> 8);  // high 8 bits

	Status s = dest_->Append(Slice(header, kNewHeaderSize));
	if (s.ok()) {
		s = dest_->Append(slice);
		if (s.ok()) {
			dest_->Flush();
		}
	}
	return s;
}

}  // namespace leveldb::log


