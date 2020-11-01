/*
 * new_env_posix.cc
 *
 *  Created on: Jul 16, 2020
 *      Author: jiaan
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "newdb/new_statusor.h"
#include "newdb/new_db_env.h"

namespace leveldb {

namespace {
	const size_t kBufferSize = 65536;  // 64 KB

	Status PosixError(const std::string& context, int error_number) {
	  if (error_number == ENOENT) {
	    return Status::NotFound(context, std::strerror(error_number));
	  } else {
	    return Status::IOError(context, std::strerror(error_number));
	  }
	}

	class NewPosixWritableFile final : public WritableFile {
		public:
			NewPosixWritableFile(const std::string& filename, int fd)
				: filename_(filename), fd_(fd), pos_(0) {}
			~NewPosixWritableFile() override {}

		  Status Append(const Slice& data) override {
		  	size_t write_size = data.size();
		  	const char* write_data = data.data();

		  	// Writes to the buffer.
		  	size_t avail = kBufferSize - pos_;
		  	size_t fit_buffer_size = std::min(avail, write_size);
		  	std::memcpy(buffer_ + pos_, write_data, fit_buffer_size);
		  	write_data += fit_buffer_size;
		  	write_size -= fit_buffer_size;
	  		pos_ += fit_buffer_size;

		  	// write_data can fully fit in the buffer.
		  	if (write_size == 0) {
		  		return Status::OK();
		  	}

		  	// Flush the buffer as it is full and there is more data to come.
		  	Status status = Flush();
		  	if (!status.ok()) {
		  		return status;
		  	}

		  	// Write to buffer if the remaining data can still fit.
		  	if (write_size < kBufferSize) {
			  	std::memcpy(buffer_, write_data, write_size);
			  	pos_ = write_size;
			  	return Status::OK();
		  	}

		  	// Write to fd if the remaining data cannot fit in buffer.
		  	return WriteThrough(write_data, write_size);
		  }

		  Status Close() override {
		  	Status status = Flush();
		  	const int res = ::close(fd_);
		  	fd_ = -1;
		  	if (res < 0) {
		  		return Status::IOError(filename_, std::to_string(errno));
		  	}
		  	return status;
		  }

		  Status Flush() override {
		  	Status status = WriteThrough(buffer_, pos_);
		  	pos_ = 0;
		  	return status;
		  }

		  Status Sync() override {
		  	Status status = Flush();
		  	if (!status.ok()) {
		  		return status;
		  	}

		  	if (::fsync(fd_) != 0) {
		  		return PosixError(filename_, errno);
		  	}

		  	return Status::OK();
		  }

		private:
		  Status WriteThrough(const char* data, size_t size) {
		  	// Is there a way to do a transaction here? Either success or fail.
		  	while (size > 0) {
		  		ssize_t res = ::write(fd_, data, size);
		  		if (res < 0) {
		  			return Status::IOError(filename_, std::to_string(errno));
		  		}
		  		size -= res;
		  	}
		  	return Status::OK();
		  };

			int fd_;
			size_t pos_;
			const std::string filename_;
			char buffer_[kBufferSize];
	};

	class NewPosixSequentialFile final : public SequentialFile {
		public:
			NewPosixSequentialFile(const std::string& filename, int fd)
				: filename_(filename), fd_(fd) {}
			~NewPosixSequentialFile() {}

		  Status Read(size_t n, Slice* result, char* scratch) {
		  	// what if scratch is a local variable??
		  	::ssize_t read_size = ::read(fd_, scratch, n);
		  	if (read_size < 0) {
		  		return PosixError(filename_, errno);
		  	}
		  	if (read_size == 0) {
		  		return Status::NotFound(Slice());
		  	}

		  	*result = Slice(scratch, read_size);
		  	return Status::OK();
		  }

		  Status Skip(uint64_t n) {
		    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
		      return PosixError(filename_, errno);
		    }
		    return Status::OK();
		  }

		private:
			int fd_;
			const std::string filename_;
	};

	class NewPosixEnv final : public Env {
		public:
			~NewPosixEnv() {

			}

			Status NewSequentialFile(const std::string& fname,
																			 SequentialFile** result) {
				int fd = ::open(fname.c_str(), O_RDONLY);
				if (fd < 0) {
					*result = nullptr;
					return Status::IOError(fname, std::to_string(errno));
				}

				*result = new NewPosixSequentialFile(fname, fd);
				return Status::OK();
			}

			Status NewRandomAccessFile(const std::string& fname,
																				 RandomAccessFile** result) {
				return Status::NotSupported(Slice());
			}

			Status NewWritableFile(const std::string& fname,
																		 WritableFile** result) {
				int fd = ::open(fname.c_str(),
												O_TRUNC | O_WRONLY | O_CREAT, 0644);
				if (fd < 0) {
					*result = nullptr;
					return Status::IOError(fname, std::to_string(errno));
				}

				*result = new NewPosixWritableFile(fname, fd);
				return Status::OK();
			}

			Status NewAppendableFile(const std::string& fname,
																			 WritableFile** result) {
				int fd = ::open(fname.c_str(),
												O_APPEND | O_WRONLY | O_CREAT, 0644);
				if (fd < 0) {
					*result = nullptr;
					return Status::IOError(fname, std::to_string(errno));
				}

				*result = new NewPosixWritableFile(fname, fd);
				return Status::OK();
			}

			bool FileExists(const std::string& filename) override {
				return ::access(filename.c_str(), F_OK) == 0;
			}

			Status GetChildren(const std::string& directory_path,
												 std::vector<std::string>* result) override {
				result->clear();
				::DIR* dir = ::opendir(directory_path.c_str());
				if (dir == nullptr) {
					return PosixError(directory_path, errno);
				}
				struct ::dirent* entry;
				while ((entry = ::readdir(dir)) != nullptr) {
					result->emplace_back(entry->d_name);
				}
				::closedir(dir);
				return Status::OK();
			}

			Status RemoveFile(const std::string& filename) override {
				if (::unlink(filename.c_str()) != 0) {
					return PosixError(filename, errno);
				}
				return Status::OK();
			}

			Status CreateDir(const std::string& dirname) override {
				if (::mkdir(dirname.c_str(), 0755) != 0) {
					return PosixError(dirname, errno);
				}
				return Status::OK();
			}

			Status RemoveDir(const std::string& dirname) override {
				if (::rmdir(dirname.c_str()) != 0) {
					return PosixError(dirname, errno);
				}
				return Status::OK();
			}

			Status GetFileSize(const std::string& filename, uint64_t* size) override {
				struct ::stat file_stat;
				if (::stat(filename.c_str(), &file_stat) != 0) {
					*size = 0;
					return PosixError(filename, errno);
				}
				*size = file_stat.st_size;
				return Status::OK();
			}

			Status RenameFile(const std::string& from, const std::string& to) override {
				if (std::rename(from.c_str(), to.c_str()) != 0) {
					return PosixError(from, errno);
				}
				return Status::OK();
			}
			Status LockFile(const std::string& fname, FileLock** lock) {

			}

			Status UnlockFile(FileLock* lock) {

			}

			void Schedule(void (*function)(void* arg), void* arg) {

			}

			void StartThread(void (*function)(void* arg), void* arg) {

			}

			Status GetTestDirectory(std::string* path) {
		    const char* env = std::getenv("TEST_TMPDIR");
		    if (env && env[0] != '\0') {
		      *path = env;
		    } else {
		      char buf[100];
		      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
		                    static_cast<int>(::geteuid()));
		      *path = buf;
		    }

		    ::mkdir(path->c_str(), 0755);

		    return Status::OK();
			}

			Status NewLogger(const std::string& fname, Logger** result) {

			}

			uint64_t NowMicros() {

			}

			void SleepForMicroseconds(int micros) {

			}
	};
}  // namespace

NewDBEnv* CreateNewDBEnv() {
	static NewDBEnv* env = new NewDBEnv();
	return env;
}

Status ReadFileToStringFromNewDBEnv(NewDBEnv* env, const std::string& fname,
                                     std::string* data) {
  data->clear();
  const auto& r_res = env->NewSequentialFile(fname);
  Status s = r_res.status();
  if (!s.ok()) {
    return s;
  }
  auto* file = r_res.value();
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  return s;
}

/** Declarations of NewDBEnv methods **/
StatusOr<SequentialFile> NewDBEnv::NewSequentialFile(const std::string& fname) {
	int fd = ::open(fname.c_str(), O_RDONLY);
	if (fd < 0) {
		return Status::IOError(fname, std::to_string(errno));
	}

	return std::unique_ptr<SequentialFile>(new NewPosixSequentialFile(fname, fd));
}

StatusOr<WritableFile> NewDBEnv::NewWritableFile(const std::string& fname) {
	int fd = ::open(fname.c_str(),
									O_TRUNC | O_WRONLY | O_CREAT, 0644);
	if (fd < 0) {
		return Status::IOError(fname, std::to_string(errno));
	}

	return std::unique_ptr<WritableFile>(new NewPosixWritableFile(fname, fd));
}

StatusOr<WritableFile> NewDBEnv::NewRandomAccessFile(const std::string& fname) {
	return Status::NotSupported(Slice());
}

StatusOr<WritableFile> NewDBEnv::NewAppendableFile(const std::string& fname) {
	int fd = ::open(fname.c_str(),
									O_APPEND | O_WRONLY | O_CREAT, 0644);
	if (fd < 0) {
		return Status::IOError(fname, std::to_string(errno));
	}

	return std::unique_ptr<WritableFile>(new NewPosixWritableFile(fname, fd));
}

Status NewDBEnv::GetTestDirectory(std::string* path) {
  const char* env = std::getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *path = env;
  } else {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                  static_cast<int>(::geteuid()));
    *path = buf;
  }

  ::mkdir(path->c_str(), 0755);

  return Status::OK();
}

bool NewDBEnv::FileExists(const std::string& filename) {
	return ::access(filename.c_str(), F_OK) == 0;
}

Status NewDBEnv::GetChildren(const std::string& directory_path,
									 std::vector<std::string>* result) {
	result->clear();
	::DIR* dir = ::opendir(directory_path.c_str());
	if (dir == nullptr) {
		return PosixError(directory_path, errno);
	}
	struct ::dirent* entry;
	while ((entry = ::readdir(dir)) != nullptr) {
		result->emplace_back(entry->d_name);
	}
	::closedir(dir);
	return Status::OK();
}

Status NewDBEnv::RemoveFile(const std::string& filename) {
	if (::unlink(filename.c_str()) != 0) {
		return PosixError(filename, errno);
	}
	return Status::OK();
}

Status NewDBEnv::CreateDir(const std::string& dirname) {
	if (::mkdir(dirname.c_str(), 0755) != 0) {
		return PosixError(dirname, errno);
	}
	return Status::OK();
}

Status NewDBEnv::RemoveDir(const std::string& dirname) {
	if (::rmdir(dirname.c_str()) != 0) {
		return PosixError(dirname, errno);
	}
	return Status::OK();
}

Status NewDBEnv::GetFileSize(const std::string& filename, uint64_t* size) {
	struct ::stat file_stat;
	if (::stat(filename.c_str(), &file_stat) != 0) {
		*size = 0;
		return PosixError(filename, errno);
	}
	*size = file_stat.st_size;
	return Status::OK();
}

Status NewDBEnv::RenameFile(const std::string& from, const std::string& to) {
	if (std::rename(from.c_str(), to.c_str()) != 0) {
		return PosixError(from, errno);
	}
	return Status::OK();
}

}  // namespace leveldb


