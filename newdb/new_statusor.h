/*
 * new_statusor.h
 *
 *  Created on: Sep 5, 2020
 *      Author: jiaan
 */

#ifndef LEVELDB_NEWDB_NEW_STATUSOR_H_
#define LEVELDB_NEWDB_NEW_STATUSOR_H_

#include <memory>

#include "leveldb/status.h"
#include "leveldb/env.h"

namespace leveldb {

template <typename T>
class StatusOr {

	public:
		StatusOr() : value_(nullptr), status_(Status::NotSupported(Slice())) {}
		StatusOr(std::unique_ptr<T> value, Status status);
		StatusOr(std::unique_ptr<T> value);
		StatusOr(Status status);

		bool ok() const;

		const Status& status() const;
		T* value() const;

	private:
		std::unique_ptr<T> value_;
		Status status_;
};

template <class T>
StatusOr<T>::StatusOr(std::unique_ptr<T> value, Status status)
	: value_(std::move(value)), status_(std::move(status)) {}

template <class T>
StatusOr<T>::StatusOr(std::unique_ptr<T> value)
	: value_(std::move(value)), status_(Status::OK()) {}

// TODO: only non-ok status
template <class T>
StatusOr<T>::StatusOr(Status status)
	: status_(std::move(status)) {}

template <class T>
bool StatusOr<T>::ok() const {
	return status_.ok();
}

template <class T>
const Status& StatusOr<T>::status() const {
	return status_;
}

template <class T>
T* StatusOr<T>::value() const {
	return &(*value_);
}

}  //

#endif /* LEVELDB_NEWDB_NEW_STATUSOR_H_ */
