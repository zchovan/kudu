// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <fts.h>
#include <glob.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/once.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/array_view.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/malloc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/openssl_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/trace.h"

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#include <sys/sysctl.h>
#else
#include <linux/falloc.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <linux/ioctl.h>
#include <linux/kernel.h>
#include <linux/magic.h>
#include <linux/types.h>
#include <sys/ioctl.h>
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#endif  // defined(__APPLE__)

using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using std::accumulate;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

// Copied from falloc.h. Useful for older kernels that lack support for
// hole punching; fallocate(2) will return EOPNOTSUPP.
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE 0x01 /* default is extend size */
#endif
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE  0x02 /* de-allocates range */
#endif


#ifndef __APPLE__
// These struct and ioctl definitions were copied verbatim from xfsprogs.
typedef struct xfs_flock64 {
        __s16           l_type;
        __s16           l_whence;
        __s64           l_start;
        __s64           l_len;          /* len == 0 means until end of file */
        __s32           l_sysid;
        __u32           l_pid;
        __s32           l_pad[4];       /* reserve area                     */
} xfs_flock64_t;
#define XFS_IOC_UNRESVSP64      _IOW ('X', 43, struct xfs_flock64)
#endif

// OSX does not have fdatasync or fread_unlocked.
#ifdef __APPLE__
#define fdatasync fsync
#define fread_unlocked fread
#endif

// With some probability, if 'filename_expr' matches the glob pattern specified
// by the 'env_inject_eio_globs' flag, calls RETURN_NOT_OK on 'error_expr'.
#define MAYBE_RETURN_EIO(filename_expr, error_expr) do { \
  const string& f_ = (filename_expr); \
  MAYBE_RETURN_FAILURE(FLAGS_env_inject_eio, \
      ShouldInject(f_, FLAGS_env_inject_eio_globs) ? (error_expr) : Status::OK()); \
} while (0)

bool ShouldInject(const string& candidate, const string& glob_patterns) {
  // Never inject on /proc/ file accesses regardless of the configured flag,
  // since it's not possible for /proc to "go bad".
  //
  // NB: it's important that this is done here _before_ consulting glob_patterns
  // since some background threads read /proc/ after gflags have already been
  // destructed.
  if (HasPrefixString(candidate, "/proc/")) {
    return false;
  }

  vector<string> globs = strings::Split(glob_patterns, ",", strings::SkipEmpty());
  for (const auto& glob : globs) {
    if (fnmatch(glob.c_str(), candidate.c_str(), 0) == 0) {
      return true;
    }
  }
  return false;
}

// See KUDU-588 for details.
DEFINE_bool(env_use_fsync, false,
            "Use fsync(2) instead of fdatasync(2) for synchronizing dirty "
            "data to disk.");
TAG_FLAG(env_use_fsync, advanced);
TAG_FLAG(env_use_fsync, evolving);

// See KUDU-2052 for details.
DEFINE_bool(env_use_ioctl_hole_punch_on_xfs, true,
            "Use the XFS_IOC_UNRESVSP64 ioctl instead of fallocate(2) to "
            "punch holes on XFS filesystems.");
TAG_FLAG(env_use_ioctl_hole_punch_on_xfs, advanced);
TAG_FLAG(env_use_ioctl_hole_punch_on_xfs, experimental);

DEFINE_bool(crash_on_eio, false,
            "Kill the process if an I/O operation results in EIO. If false, "
            "I/O resulting in EIOs will return the status IOError and leave "
            "error-handling up to the caller.");
TAG_FLAG(crash_on_eio, advanced);
TAG_FLAG(crash_on_eio, experimental);

DEFINE_bool(never_fsync, false,
            "Never fsync() anything to disk. This is used by certain test cases to "
            "speed up runtime. This is very unsafe to use in production.");
TAG_FLAG(never_fsync, advanced);
TAG_FLAG(never_fsync, unsafe);

DEFINE_int32(env_inject_short_read_bytes, 0,
             "The number of bytes less than the requested bytes to read");
TAG_FLAG(env_inject_short_read_bytes, hidden);
DEFINE_int32(env_inject_short_write_bytes, 0,
             "The number of bytes less than the requested bytes to write");
TAG_FLAG(env_inject_short_write_bytes, hidden);

DEFINE_double(env_inject_eio, 0.0,
              "Fraction of the time that operations on certain files will fail "
              "with the posix code EIO.");
TAG_FLAG(env_inject_eio, hidden);
DEFINE_string(env_inject_eio_globs, "*",
              "Comma-separated list of glob patterns specifying files on which "
              "I/O will fail. By default, all files may cause a failure.");
TAG_FLAG(env_inject_eio_globs, hidden);

DEFINE_string(env_inject_lock_failure_globs, "",
              "Comma-separated list of glob patterns specifying files on which "
              "attempts to obtain a file lock will fail. By default, no files "
              "will fail.");
TAG_FLAG(env_inject_lock_failure_globs, hidden);

DEFINE_bool(encrypt_data_at_rest, false,
            "Whether sensitive files should be encrypted on the file system.");
TAG_FLAG(encrypt_data_at_rest, hidden);
DEFINE_int32(encryption_key_length, 128, "Encryption key length.");
TAG_FLAG(encryption_key_length, hidden);
DEFINE_validator(encryption_key_length,
                 [](const char* /*n*/, int32 v) { return v == 128 || v == 192 || v == 256; });

static __thread uint64_t thread_local_id;
static Atomic64 cur_thread_local_id_;

namespace kudu {

const char* const Env::kInjectedFailureStatusMsg = "INJECTED FAILURE";

const uint8_t kEncryptionBlockSize = 16;

const uint8_t kEncryptionHeaderSize = 64;

const char* const kEncryptionHeaderMagic = "kuduenc";

using evp_ctx_unique_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>;

namespace {


struct FreeDeleter {
  inline void operator()(void* ptr) const {
    free(ptr);
  }
};

enum class EncryptionAlgorithm {
  AES128CTR = 0x00,
  AES192CTR = 0x01,
  AES256CTR = 0x02,
  // ECB mode below only used to encrypt keys.
  AES128ECB = 0xFD,
  AES192ECB = 0XFE,
  AES256ECB = 0xFF,
};

// The encryption header is stored on disk as follows:
//
// *----------------------------------------------*
// | "kuduenc" magic string (7 bytes)             |
// *----------------------------------------------*
// | Algorithm and key length (1 byte)            |
// *----------------------------------------------*
// | Encrypted File Key (right-padded) (32 bytes) |
// *----------------------------------------------*
// | Reserved for future use (24 bytes)           |
// *----------------------------------------------*
//
// The algorithm and key length mapping is:
//
// *------*-------------*
// | 0x00 | AES-128-CTR |
// *------*-------------*
// | 0x01 | AES-192-CTR |
// *------*-------------*
// | 0x02 | AES-256-CTR |
// *------*-------------*
struct EncryptionHeader {
  EncryptionAlgorithm algorithm;
  uint8_t key[32];
};

// KUDU-3316: This is the key temporarily used for all encrypion. Obviously,
// this is not secure and MUST be removed and replaced with real keys once the
// key infra is in place.
// TODO(abukor): delete this.
const struct EncryptionHeader kDummyEncryptionKey = {
  EncryptionAlgorithm::AES128ECB,
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 42},
};

const EVP_CIPHER* GetEVPCipher(EncryptionAlgorithm algorithm) {
  switch (algorithm) {
    case EncryptionAlgorithm::AES128CTR:
      return EVP_aes_128_ctr();
    case EncryptionAlgorithm::AES192CTR:
      return EVP_aes_192_ctr();
    case EncryptionAlgorithm::AES256CTR:
      return EVP_aes_256_ctr();
    case EncryptionAlgorithm::AES128ECB:
      return EVP_aes_128_ecb();
    case EncryptionAlgorithm::AES192ECB:
      return EVP_aes_192_ecb();
    case EncryptionAlgorithm::AES256ECB:
      return EVP_aes_256_ecb();
    default:
      return nullptr;
  }
}

#if defined(__APPLE__)
// Simulates Linux's fallocate file preallocation API on OS X.
int fallocate(int fd, int mode, off_t offset, off_t len) {
  CHECK_EQ(mode, 0);
  off_t size = offset + len;

  struct stat stat;
  int ret = fstat(fd, &stat);
  if (ret < 0) {
    return ret;
  }

  if (stat.st_blocks * 512 < size) {
    // The offset field seems to have no effect; the file is always allocated
    // with space from 0 to the size. This is probably because OS X does not
    // support sparse files.
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, size};
    if (fcntl(fd, F_PREALLOCATE, &store) < 0) {
      LOG(INFO) << "Unable to allocate contiguous disk space, attempting non-contiguous allocation";
      store.fst_flags = F_ALLOCATEALL;
      ret = fcntl(fd, F_PREALLOCATE, &store);
      if (ret < 0) {
        return ret;
      }
    }
  }

  if (stat.st_size < size) {
    // fcntl does not change the file size, so set it if necessary.
    int ret;
    RETRY_ON_EINTR(ret, ftruncate(fd, size));
    return ret;
  }
  return 0;
}

// Implementations for `preadv` and `pwritev` are available in the MacOSX11+ SDK.
// However, we still use the simulated implementations in order to support older versions.

// Simulates Linux's preadv API on OS X.
ssize_t preadvsim(int fd, const struct iovec* iovec, int count, off_t offset) {
  ssize_t total_read_bytes = 0;
  for (int i = 0; i < count; i++) {
    ssize_t r;
    RETRY_ON_EINTR(r, pread(fd, iovec[i].iov_base, iovec[i].iov_len, offset));
    if (r < 0) {
      return r;
    }
    total_read_bytes += r;
    if (static_cast<size_t>(r) < iovec[i].iov_len) {
      break;
    }
    offset += iovec[i].iov_len;
  }
  return total_read_bytes;
}

// Simulates Linux's pwritev API on OS X.
ssize_t pwritevsim(int fd, const struct iovec* iovec, int count, off_t offset) {
  ssize_t total_written_bytes = 0;
  for (int i = 0; i < count; i++) {
    ssize_t r;
    RETRY_ON_EINTR(r, pwrite(fd, iovec[i].iov_base, iovec[i].iov_len, offset));
    if (r < 0) {
      return r;
    }
    total_written_bytes += r;
    if (static_cast<size_t>(r) < iovec[i].iov_len) {
      break;
    }
    offset += iovec[i].iov_len;
  }
  return total_written_bytes;
}

#endif

void DoClose(int fd) {
  int err;
  RETRY_ON_EINTR(err, close(fd));
  if (PREDICT_FALSE(err != 0)) {
    PLOG(WARNING) << "Failed to close fd " << fd;
  }
}

// Close file descriptor when object goes out of scope.
class ScopedFdCloser {
 public:
  explicit ScopedFdCloser(int fd)
    : fd_(fd) {
  }

  ~ScopedFdCloser() {
    ThreadRestrictions::AssertIOAllowed();
    DoClose(fd_);
  }

 private:
  const int fd_;
};

Status IOError(const string& context, int err_number) {
  switch (err_number) {
    case ENOENT:
      return Status::NotFound(context, ErrnoToString(err_number), err_number);
    case EEXIST:
      return Status::AlreadyPresent(context, ErrnoToString(err_number), err_number);
    case EOPNOTSUPP:
      return Status::NotSupported(context, ErrnoToString(err_number), err_number);
    case EIO:
      if (FLAGS_crash_on_eio) {
        // TODO(awong): This is very, very coarse-grained. A more comprehensive
        // approach is described in KUDU-616.
        LOG(FATAL) << "Fatal I/O error, context: " << context;
      } else {
        LOG(ERROR) << "I/O error, context: " << context;
      }
  }
  return Status::IOError(context, ErrnoToString(err_number), err_number);
}

Status DoSync(int fd, const string& filename) {
  MAYBE_RETURN_EIO(filename, IOError(Env::kInjectedFailureStatusMsg, EIO));

  ThreadRestrictions::AssertIOAllowed();
  if (FLAGS_never_fsync) return Status::OK();
  if (FLAGS_env_use_fsync) {
    TRACE_COUNTER_SCOPE_LATENCY_US("fsync_us");
    TRACE_COUNTER_INCREMENT("fsync", 1);
    if (fsync(fd) < 0) {
      return IOError(filename, errno);
    }
  } else {
    TRACE_COUNTER_INCREMENT("fdatasync", 1);
    TRACE_COUNTER_SCOPE_LATENCY_US("fdatasync_us");
    if (fdatasync(fd) < 0) {
      return IOError(filename, errno);
    }
  }
  return Status::OK();
}

Status DoOpen(const string& filename, int flags, const string& reason, int* fd) {
  int f;
  RETRY_ON_EINTR(f, open(filename.c_str(), flags));
  if (f == -1) {
    return IOError(Substitute("Error opening for $0: $1", reason, filename), errno);
  }
  *fd = f;
  return Status::OK();
}

// Encrypts the data in 'cleartext' and writes it to 'ciphertext'. It requires
// 'offset' to be set in the file as it's used to set the initialization vector.
Status DoEncryptV(const EncryptionHeader* eh,
                  uint64_t offset,
                  ArrayView<const Slice> cleartext,
                  ArrayView<Slice> ciphertext) {
  DCHECK_EQ(cleartext.size(), ciphertext.size());
  // Set the initialization vector based on the offset.
  uint8_t iv[16];
  InlineBigEndianEncodeFixed64(&iv[0], 0);
  InlineBigEndianEncodeFixed64(&iv[8], offset / kEncryptionBlockSize);

  evp_ctx_unique_ptr ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);

  OPENSSL_RET_NOT_OK(EVP_EncryptInit_ex(ctx.get(), GetEVPCipher(eh->algorithm),
                                        nullptr, eh->key, iv),
                     "Failed to initialize encryption");
  size_t offset_mod = offset % kEncryptionBlockSize;
  if (offset_mod) {
    unsigned char scratch_clear[kEncryptionBlockSize];
    unsigned char scratch_cipher[kEncryptionBlockSize];
    int out_length;
    OPENSSL_RET_NOT_OK(EVP_EncryptUpdate(ctx.get(), scratch_cipher, &out_length,
                                         scratch_clear, offset_mod),
                       "Failed to encrypt scratch data");
    DCHECK_LE(out_length, kEncryptionBlockSize);
  }
  for (auto i = 0; i < cleartext.size(); ++i) {
    int out_length;
    // Normally, EVP_EncryptFinal_ex() would be needed after the last chunk of
    // data encrypted with EVP_EncryptUpdate(). In Kudu, we only use AES-CTR
    // which requires no padding or authentication tags, so
    // EVP_EncryptFinal_ex() doesn't actually add anything.
    OPENSSL_RET_NOT_OK(EVP_EncryptUpdate(ctx.get(),
                                         ciphertext[i].mutable_data(),
                                         &out_length,
                                         cleartext[i].data(),
                                         cleartext[i].size()),
                       "Failed to encrypt data");
    DCHECK_EQ(out_length, cleartext[i].size());
    DCHECK_LE(out_length, ciphertext[i].size());
  }
  return Status::OK();
}

// Decrypts 'data'. Uses 'offset' in the file to set the initialization vector.
Status DoDecryptV(const EncryptionHeader* eh, uint64_t offset, ArrayView<Slice> data) {
  // Set the initialization vector based on the offset.
  uint8_t iv[16];
  InlineBigEndianEncodeFixed64(&iv[0], 0);
  InlineBigEndianEncodeFixed64(&iv[8], offset / kEncryptionBlockSize);

  evp_ctx_unique_ptr ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  OPENSSL_RET_NOT_OK(EVP_DecryptInit_ex(ctx.get(), GetEVPCipher(eh->algorithm),
                                        nullptr, eh->key, iv),
                     "Failed to initialize decryption");
  size_t offset_mod = offset % kEncryptionBlockSize;
  if (offset_mod) {
    unsigned char scratch_clear[kEncryptionBlockSize];
    unsigned char scratch_cipher[kEncryptionBlockSize];
    int out_length;
    OPENSSL_RET_NOT_OK(EVP_DecryptUpdate(ctx.get(),
                                         scratch_clear,
                                         &out_length,
                                         scratch_cipher,
                                         offset_mod),
                       "Failed to decrypt scratch data");
  }

  for (auto i = 0; i < data.size(); ++i) {
    const Slice& ciphertext_slice = data[i];
    int in_length = ciphertext_slice.size();
    if (!in_length || IsAllZeros(ciphertext_slice)) continue;
    int out_length;
    // We don't call EVP_DecryptFinal_ex() after EVP_DecryptUpdate() for the
    // same reason we don't call EVP_EncryptFinal_ex().
    OPENSSL_RET_NOT_OK(EVP_DecryptUpdate(ctx.get(),
                                         data[i].mutable_data(),
                                         &out_length,
                                         ciphertext_slice.data(),
                                         in_length),
                       "Failed to decrypt data");
  }
  return Status::OK();
}

Status DoOpen(const string& filename, Env::OpenMode mode, int* fd) {
  MAYBE_RETURN_EIO(filename, IOError(Env::kInjectedFailureStatusMsg, EIO));
  ThreadRestrictions::AssertIOAllowed();
  int flags = O_RDWR;
  switch (mode) {
    case Env::CREATE_OR_OPEN_WITH_TRUNCATE:
      flags |= O_CREAT | O_TRUNC;
      break;
    case Env::CREATE_OR_OPEN:
      flags |= O_CREAT;
      break;
    case Env::MUST_CREATE:
      flags |= O_CREAT | O_EXCL;
      break;
    case Env::MUST_EXIST:
      break;
    default:
      return Status::NotSupported(Substitute("Unknown create mode $0", mode));
  }
  int f;
  RETRY_ON_EINTR(f, open(filename.c_str(), flags, 0666));
  if (f < 0) {
    return IOError(filename, errno);
  }
  *fd = f;
  return Status::OK();
}

Status DoReadV(
    int fd,
    const string& filename,
    uint64_t offset,
    ArrayView<Slice> results,
    const EncryptionHeader* eh) {
  MAYBE_RETURN_EIO(filename, IOError(Env::kInjectedFailureStatusMsg, EIO));
  ThreadRestrictions::AssertIOAllowed();

  // Convert the results into the iovec vector to request
  // and calculate the total bytes requested
  size_t bytes_req = 0;
  size_t iov_size = results.size();
  struct iovec iov[iov_size];
  for (size_t i = 0; i < iov_size; i++) {
    Slice& result = results[i];
    bytes_req += result.size();
    iov[i] = {result.mutable_data(), result.size()};
  }

  uint64_t cur_offset = offset;
  size_t completed_iov = 0;
  size_t rem = bytes_req;
  while (rem > 0) {
    // Never request more than IOV_MAX in one request
    size_t iov_count = std::min(iov_size - completed_iov, static_cast<size_t>(IOV_MAX));
    ssize_t r;
#if defined(__APPLE__)
    RETRY_ON_EINTR(r, preadvsim(fd, iov + completed_iov, iov_count, cur_offset));
#else
    RETRY_ON_EINTR(r, preadv(fd, iov + completed_iov, iov_count, cur_offset));
#endif
    // Fake a short read for testing
    if (PREDICT_FALSE(FLAGS_env_inject_short_read_bytes > 0 && rem == bytes_req)) {
      DCHECK_LT(FLAGS_env_inject_short_read_bytes, r);
      r -= FLAGS_env_inject_short_read_bytes;
    }

    if (PREDICT_FALSE(r < 0)) {
      // An error: return a non-ok status.
      return IOError(filename, errno);
    }
    if (PREDICT_FALSE(r == 0)) {
      // EOF.
      if (eh) {
        RETURN_NOT_OK(DoDecryptV(eh, offset, results));
      }
      return Status::EndOfFile(
          Substitute("EOF trying to read $0 bytes at offset $1", bytes_req, offset));
    }
    if (PREDICT_TRUE(r == rem)) {
      // All requested bytes were read. This is almost always the case.
      if (eh) {
        RETURN_NOT_OK(DoDecryptV(eh, offset, results));
      }
      return Status::OK();
    }
    DCHECK_LE(r, rem);
    // Adjust iovec vector based on bytes read for the next request
    ssize_t bytes_rem = r;
    for (size_t i = completed_iov; i < iov_size; i++) {
      if (bytes_rem >= iov[i].iov_len) {
        // The full length of this iovec was read
        completed_iov++;
        bytes_rem -= iov[i].iov_len;
      } else {
        // Partially read this result.
        // Adjust the iov_len and iov_base to request only the missing data.
        iov[i].iov_base = static_cast<uint8_t *>(iov[i].iov_base) + bytes_rem;
        iov[i].iov_len -= bytes_rem;
        break; // Don't need to adjust remaining iovec's
      }
    }
    cur_offset += r;
    rem -= r;
  }
  if (eh) {
    RETURN_NOT_OK(DoDecryptV(eh, offset, results));
  }
  DCHECK_EQ(0, rem);
  return Status::OK();
}

Status DoWriteV(
    int fd,
    const string& filename,
    uint64_t offset,
    ArrayView<const Slice> data,
    const EncryptionHeader* eh) {
  MAYBE_RETURN_EIO(filename, IOError(Env::kInjectedFailureStatusMsg, EIO));
  ThreadRestrictions::AssertIOAllowed();

  // Convert the results into the iovec vector to request
  // and calculate the total bytes requested.
  size_t bytes_req = 0;
  size_t iov_size = data.size();
  struct iovec iov[iov_size];
  std::vector<Slice> encrypted_data(iov_size);
  SCOPED_CLEANUP({
    if (eh) {
      delete[] encrypted_data[0].mutable_data();
    }
  });
  if (eh) {
    for (size_t i = 0; i < iov_size; i++) {
      bytes_req += data[i].size();
    }
    unique_ptr<unsigned char[]> encrypted_buf(new unsigned char[bytes_req]);
    size_t buffer_offset = 0;
    for (size_t i = 0; i < iov_size; i++) {
      size_t size = data[i].size();
      encrypted_data[i] = Slice(&encrypted_buf[buffer_offset], size);
      buffer_offset += size;
      iov[i] = {const_cast<uint8_t*>(encrypted_data[i].data()), size};
    }
    RETURN_NOT_OK(DoEncryptV(eh, offset, data, encrypted_data));
    encrypted_buf.release();
  } else {
    for (size_t i = 0; i < iov_size; i++) {
      const Slice& result = data[i];
      bytes_req += result.size();
      iov[i] = {const_cast<uint8_t*>(result.data()), result.size()};
    }
  }

  uint64_t cur_offset = offset;
  size_t completed_iov = 0;
  size_t rem = bytes_req;
  while (rem > 0) {
    // Never request more than IOV_MAX in one request.
    size_t iov_count = std::min(iov_size - completed_iov, static_cast<size_t>(IOV_MAX));
    ssize_t w;
#if defined(__APPLE__)
    RETRY_ON_EINTR(w, pwritevsim(fd, iov + completed_iov, iov_count, cur_offset));
#else
    RETRY_ON_EINTR(w, pwritev(fd, iov + completed_iov, iov_count, cur_offset));
#endif
    // Fake a short write for testing.
    if (PREDICT_FALSE(FLAGS_env_inject_short_write_bytes > 0 && rem == bytes_req)) {
      DCHECK_LT(FLAGS_env_inject_short_write_bytes, w);
      w -= FLAGS_env_inject_short_read_bytes;
    }

    if (PREDICT_FALSE(w < 0)) {
      // An error: return a non-ok status.
      return IOError(filename, errno);
    }

    DCHECK_LE(w, rem);

    if (PREDICT_TRUE(w == rem)) {
      // All requested bytes were read. This is almost always the case.
      return Status::OK();
    }
    // Adjust iovec vector based on bytes read for the next request.
    ssize_t bytes_rem = w;
    for (size_t i = completed_iov; i < iov_size; i++) {
      if (bytes_rem >= iov[i].iov_len) {
        // The full length of this iovec was written.
        completed_iov++;
        bytes_rem -= iov[i].iov_len;
      } else {
        // Partially wrote this result.
        // Adjust the iov_len and iov_base to write only the missing data.
        iov[i].iov_base = static_cast<uint8_t *>(iov[i].iov_base) + bytes_rem;
        iov[i].iov_len -= bytes_rem;
        break; // Don't need to adjust remaining iovec's.
      }
    }
    cur_offset += w;
    rem -= w;
  }
  DCHECK_EQ(0, rem);
  return Status::OK();
}

Status GenerateHeader(EncryptionHeader* eh) {
  switch (FLAGS_encryption_key_length) {
    case 128:
      eh->algorithm = EncryptionAlgorithm::AES128CTR;
      break;
    case 192:
      eh->algorithm = EncryptionAlgorithm::AES192CTR;
      break;
    case 256:
      eh->algorithm = EncryptionAlgorithm::AES256CTR;
      break;
    default:
      return Status::InvalidArgument(
          "Supported key lengths for AES encryption are 128, 192, and 256.");
  }
  OPENSSL_RET_NOT_OK(RAND_bytes(eh->key, FLAGS_encryption_key_length / 8),
                     "Failed to generate random key");

  return Status::OK();
}

Status WriteEncryptionHeader(int fd, const string& filename, const EncryptionHeader& eh) {
  vector<Slice> headerv = { kEncryptionHeaderMagic };
  uint32_t key_size;
  uint8_t algorithm[1];
  switch (eh.algorithm) {
    case EncryptionAlgorithm::AES128CTR:
      algorithm[0] = 0;
      key_size = 16;
      break;
    case EncryptionAlgorithm::AES192CTR:
      algorithm[0] = 1;
      // As the keys are encrypted in ECB mode which requires padding, we need
      // 32 bytes instead of 24 to encrypt and write a 192-bit key.
      key_size = 32;
      break;
    case EncryptionAlgorithm::AES256CTR:
      algorithm[0] = 2;
      key_size = 32;
      break;
    default:
      return Status::InvalidArgument(Substitute("Unknown encryption algorithm: $0", algorithm));
  }
  headerv.emplace_back(Slice(algorithm, 1));
  Slice file_key(eh.key, key_size);

  uint8_t encrypted_file_key[32];
  Slice efk(encrypted_file_key, key_size);
  vector<Slice> clear = {file_key};
  vector<Slice> cipher = {efk};
  RETURN_NOT_OK(DoEncryptV(&kDummyEncryptionKey, 0, clear, cipher));

  // Add the encrypted file key and trailing zeros to the header.
  headerv.emplace_back(efk);
  static const uint8_t padding[40] = {0};
  // 7 bytes of magic + 1 byte of algorithm and key length.
  constexpr int kMagicAndAlgorithmSize = 8;
  Slice padding_slice(padding, kEncryptionHeaderSize - kMagicAndAlgorithmSize - key_size);
  headerv.emplace_back(padding_slice);
  return DoWriteV(fd, filename, 0, headerv, nullptr);
}

Status DoIsOnXfsFilesystem(const string& path, bool* result) {
#ifdef __APPLE__
  *result = false;
#else
  struct statfs buf;
  int ret;
  RETRY_ON_EINTR(ret, statfs(path.c_str(), &buf));
  if (ret == -1) {
    return IOError(Substitute("statfs: $0", path), errno);
  }
  // This magic number isn't defined in any header but is the value of the
  // US-ASCII string 'XFSB' expressed in hexadecimal.
  *result = (buf.f_type == 0x58465342);
#endif
  return Status::OK();
}

Status ReadEncryptionHeader(int fd, const string& filename, EncryptionHeader* eh) {
  char magic[7];
  uint8_t algorithm[1];
  char file_key[32];
  vector<Slice> headerv({ Slice(magic, 7), Slice(algorithm, 1), Slice(file_key, 32) });
  RETURN_NOT_OK(DoReadV(fd, filename, 0, headerv, nullptr));
  if (strncmp(magic, kEncryptionHeaderMagic, 7) != 0) {
    return Status::Corruption(Substitute("Invalid encryption header: $0", magic));
  }
  uint16_t key_size;
  eh->algorithm = EncryptionAlgorithm(algorithm[0]);
  switch (eh->algorithm) {
    case EncryptionAlgorithm::AES128CTR:
      key_size = 16;
      break;
    case EncryptionAlgorithm::AES192CTR:
      key_size = 24;
      break;
    case EncryptionAlgorithm::AES256CTR:
      key_size = 32;
      break;
    default:
      return Status::Corruption(Substitute("Unknown encryption algorithm: $0", algorithm));
  }
  // Round up to the nearest multiple of 16 bytes when reading and decrypting
  // the file. The actual key size can be used when storing the key in memory.
  // See WriteEncryptionHeader for more info.
  vector<Slice> v = {Slice(file_key, (key_size + 15) & -16)};
  RETURN_NOT_OK(DoDecryptV(&kDummyEncryptionKey, 0, v));
  memcpy(&eh->key, file_key, key_size);
  return Status::OK();
}

const char* ResourceLimitTypeToString(Env::ResourceLimitType t) {
  switch (t) {
    case Env::ResourceLimitType::OPEN_FILES_PER_PROCESS:
      return "open files per process";
    case Env::ResourceLimitType::RUNNING_THREADS_PER_EUID:
      return "running threads per effective uid";
    default: LOG(FATAL) << "Unknown resource limit type";
  }
  __builtin_unreachable();
}

int ResourceLimitTypeToUnixRlimit(Env::ResourceLimitType t) {
  switch (t) {
    case Env::ResourceLimitType::OPEN_FILES_PER_PROCESS: return RLIMIT_NOFILE;
    case Env::ResourceLimitType::RUNNING_THREADS_PER_EUID: return RLIMIT_NPROC;
    default: LOG(FATAL) << "Unknown resource limit type: " << t;
  }
  __builtin_unreachable();
}

#ifdef __APPLE__
const char* ResourceLimitTypeToMacosRlimit(Env::ResourceLimitType t) {
  switch (t) {
    case Env::ResourceLimitType::OPEN_FILES_PER_PROCESS:
      return "kern.maxfilesperproc";
    case Env::ResourceLimitType::RUNNING_THREADS_PER_EUID:
      return "kern.maxprocperuid";
    default: LOG(FATAL) << "Unknown resource limit type: " << t;
  }
  __builtin_unreachable();
}
#endif

class PosixFifo : public Fifo {
 public:
  explicit PosixFifo(string fname) : filename_(std::move(fname)) {}

  size_t GetEncryptionHeaderSize() const override {
    return 0;
  }

  const string& filename() const override {
    return filename_;
  }

  Status OpenForReads() override {
    CHECK_EQ(-1, read_fd_);
    return DoOpen(filename_, O_RDONLY, "reads", &read_fd_);
  }

  Status OpenForWrites() override {
    CHECK_EQ(-1, write_fd_);
    return DoOpen(filename_, O_WRONLY, "writes", &write_fd_);
  }

  int read_fd() const override {
    CHECK_NE(-1, read_fd_);
    return read_fd_;
  }

  int write_fd() const override {
    CHECK_NE(-1, write_fd_);
    return write_fd_;
  }

  ~PosixFifo() {
    if (read_fd_ != -1) {
      DoClose(read_fd_);
    }
    if (write_fd_ != -1) {
      DoClose(write_fd_);
    }
  }

 private:
  const string filename_;
  int read_fd_ = -1;
  int write_fd_ = -1;
};

class PosixSequentialFile: public SequentialFile {
 private:
  const string filename_;
  FILE* const file_;
  const bool encrypted_;
  size_t offset_;
  const EncryptionHeader encryption_header_;

 public:
  PosixSequentialFile(string fname, bool encrypted, FILE* f, EncryptionHeader eh)
    : filename_(std::move(fname)),
      file_(f),
      encrypted_(encrypted),
      offset_(encrypted ? kEncryptionHeaderSize : 0),
      encryption_header_(eh) {}

  ~PosixSequentialFile() {
    int err;
    RETRY_ON_EINTR(err, fclose(file_));
    if (PREDICT_FALSE(err != 0)) {
      PLOG(WARNING) << "Failed to close " << filename_;
    }
  }

  virtual Status Read(Slice* result) OVERRIDE {
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    size_t r;
    STREAM_RETRY_ON_EINTR(r, file_, fread_unlocked(result->mutable_data(), 1,
                                                   result->size(), file_));
    if (r < result->size()) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file.
        // We need to adjust the slice size.
        result->truncate(r);
      } else {
        // A partial read with an error: return a non-ok status.
        return IOError(filename_, errno);
      }
    }
    if (encrypted_) {
      RETURN_NOT_OK(DoDecryptV(&encryption_header_, offset_, ArrayView<Slice>(result, 1)));
    }
    offset_ += r;
    return Status::OK();
  }

  virtual Status Skip(uint64_t n) OVERRIDE {
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    TRACE_EVENT1("io", "PosixSequentialFile::Skip", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    offset_ += n;
    return Status::OK();
  }

  virtual const string& filename() const OVERRIDE { return filename_; }

  size_t GetEncryptionHeaderSize() const override {
    return encrypted_ ? kEncryptionHeaderSize : 0;
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  const string filename_;
  const int fd_;
  const bool encrypted_;
  const EncryptionHeader encryption_header_;

 public:
  PosixRandomAccessFile(string fname, int fd, bool encrypted, EncryptionHeader eh)
      : filename_(std::move(fname)),
        fd_(fd),
        encrypted_(encrypted),
        encryption_header_(eh) {}
  ~PosixRandomAccessFile() {
    DoClose(fd_);
  }

  virtual Status Read(uint64_t offset, Slice result) const OVERRIDE {
    DCHECK_GE(offset, GetEncryptionHeaderSize());
    return DoReadV(fd_, filename_, offset, ArrayView<Slice>(&result, 1),
                   encrypted_ ? &encryption_header_ : nullptr);
  }

  virtual Status ReadV(uint64_t offset, ArrayView<Slice> results) const OVERRIDE {
    DCHECK_GE(offset, GetEncryptionHeaderSize());
    return DoReadV(fd_, filename_, offset, results,
                   encrypted_ ? &encryption_header_ : nullptr);
  }

  virtual Status Size(uint64_t *size) const OVERRIDE {
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    TRACE_EVENT1("io", "PosixRandomAccessFile::Size", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    struct stat st;
    if (fstat(fd_, &st) == -1) {
      return IOError(filename_, errno);
    }
    *size = st.st_size;
    return Status::OK();
  }

  virtual const string& filename() const OVERRIDE { return filename_; }

  size_t GetEncryptionHeaderSize() const override {
    return encrypted_ ? kEncryptionHeaderSize : 0;
  }

  virtual size_t memory_footprint() const OVERRIDE {
    return kudu_malloc_usable_size(this) + filename_.capacity();
  }
};

// Use non-memory mapped POSIX files to write data to a file.
//
// TODO (perf) investigate zeroing a pre-allocated allocated area in
// order to further improve Sync() performance.
class PosixWritableFile : public WritableFile {
 public:
  PosixWritableFile(string fname, int fd, uint64_t file_size, bool sync_on_close,
                     bool encrypted, EncryptionHeader eh)
      : filename_(std::move(fname)),
        fd_(fd),
        sync_on_close_(sync_on_close),
        filesize_(file_size),
        pre_allocated_size_(0),
        pending_sync_(false),
        closed_(false),
        encrypted_(encrypted),
        encryption_header_(eh) {}

  ~PosixWritableFile() {
    WARN_NOT_OK(Close(), "Failed to close " + filename_);
  }

  virtual Status Append(const Slice& data) OVERRIDE {
    return AppendV(ArrayView<const Slice>(&data, 1));
  }

  virtual Status AppendV(ArrayView<const Slice> data) OVERRIDE {
    ThreadRestrictions::AssertIOAllowed();
    RETURN_NOT_OK(DoWriteV(fd_, filename_, filesize_, data,
                           encrypted_ ? &encryption_header_ : nullptr));
    // Calculate the amount of data written
    size_t bytes_written = accumulate(data.begin(), data.end(), static_cast<size_t>(0),
                                      [&](int sum, const Slice& curr) {
                                        return sum + curr.size();
                                      });
    filesize_ += bytes_written;
    pending_sync_ = true;
    return Status::OK();
  }

  virtual Status PreAllocate(uint64_t size) OVERRIDE {
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));

    TRACE_EVENT1("io", "PosixWritableFile::PreAllocate", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    uint64_t offset = std::max(filesize_, pre_allocated_size_);
    int ret;
    RETRY_ON_EINTR(ret, fallocate(fd_, 0, offset, size));
    if (ret != 0) {
      if (errno == EOPNOTSUPP) {
        KLOG_FIRST_N(WARNING, 1) << "The filesystem does not support fallocate().";
      } else if (errno == ENOSYS) {
        KLOG_FIRST_N(WARNING, 1) << "The kernel does not implement fallocate().";
      } else {
        return IOError(filename_, errno);
      }
    }
    pre_allocated_size_ = offset + size;
    return Status::OK();
  }

  virtual Status Close() OVERRIDE {
    if (closed_) {
      return Status::OK();
    }
    TRACE_EVENT1("io", "PosixWritableFile::Close", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    Status s;

    // If we've allocated more space than we used, truncate to the
    // actual size of the file and perform Sync().
    if (filesize_ < pre_allocated_size_) {
      int ret;
      RETRY_ON_EINTR(ret, ftruncate(fd_, filesize_));
      if (ret != 0) {
        s = IOError(filename_, errno);
        pending_sync_ = true;
      }
    }

    if (sync_on_close_) {
      Status sync_status = Sync();
      if (!sync_status.ok()) {
        LOG(ERROR) << "Unable to Sync " << filename_ << ": " << sync_status.ToString();
        if (s.ok()) {
          s = sync_status;
        }
      }
    }

    int ret;
    RETRY_ON_EINTR(ret, close(fd_));
    if (ret < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    closed_ = true;
    return s;
  }

  virtual Status Flush(FlushMode mode) OVERRIDE {
    TRACE_EVENT1("io", "PosixWritableFile::Flush", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
#if defined(__linux__)
    int flags = SYNC_FILE_RANGE_WRITE;
    if (mode == FLUSH_SYNC) {
      flags |= SYNC_FILE_RANGE_WAIT_BEFORE;
      flags |= SYNC_FILE_RANGE_WAIT_AFTER;
    }
    if (sync_file_range(fd_, 0, 0, flags) < 0) {
      return IOError(filename_, errno);
    }
#else
    if (mode == FLUSH_SYNC && fsync(fd_) < 0) {
      return IOError(filename_, errno);
    }
#endif
    return Status::OK();
  }

  virtual Status Sync() OVERRIDE {
    TRACE_EVENT1("io", "PosixWritableFile::Sync", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    LOG_SLOW_EXECUTION(WARNING, 1000, Substitute("sync call for $0", filename_)) {
      if (pending_sync_) {
        pending_sync_ = false;
        RETURN_NOT_OK(DoSync(fd_, filename_));
      }
    }
    return Status::OK();
  }

  virtual uint64_t Size() const OVERRIDE {
    return filesize_;
  }

  virtual const string& filename() const OVERRIDE { return filename_; }

  size_t GetEncryptionHeaderSize() const override {
    return encrypted_ ? kEncryptionHeaderSize : 0;
  }

 private:
  const string filename_;
  const int fd_;
  const bool sync_on_close_;

  uint64_t filesize_;
  uint64_t pre_allocated_size_;
  bool pending_sync_;
  bool closed_;
  const bool encrypted_;
  const EncryptionHeader encryption_header_;
};

class PosixRWFile : public RWFile {
 public:
  PosixRWFile(string fname, int fd, bool sync_on_close, bool encrypted,
              EncryptionHeader eh)
      : filename_(std::move(fname)),
        fd_(fd),
        sync_on_close_(sync_on_close),
        is_on_xfs_(false),
        closed_(false),
        encrypted_(encrypted),
        encryption_header_(eh) {}

  ~PosixRWFile() {
    WARN_NOT_OK(Close(), "Failed to close " + filename_);
  }

  virtual Status Read(uint64_t offset, Slice result) const OVERRIDE {
    DCHECK_GE(offset, GetEncryptionHeaderSize());
    return DoReadV(fd_, filename_, offset, ArrayView<Slice>(&result, 1),
                   encrypted_ ? &encryption_header_ : nullptr);
  }

  virtual Status ReadV(uint64_t offset, ArrayView<Slice> results) const OVERRIDE {
    DCHECK_GE(offset, GetEncryptionHeaderSize());
    return DoReadV(fd_, filename_, offset, results,
                   encrypted_ ? &encryption_header_ : nullptr);
  }

  virtual Status Write(uint64_t offset, const Slice& data) OVERRIDE {
    return WriteV(offset, ArrayView<const Slice>(&data, 1));
  }

  virtual Status WriteV(uint64_t offset, ArrayView<const Slice> data) OVERRIDE {
    DCHECK_GE(offset, GetEncryptionHeaderSize());
    return DoWriteV(fd_, filename_, offset, data,
                    encrypted_ ? &encryption_header_ : nullptr);
  }

  virtual Status PreAllocate(uint64_t offset,
                             size_t length,
                             PreAllocateMode mode) OVERRIDE {
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));

    TRACE_EVENT1("io", "PosixRWFile::PreAllocate", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    int falloc_mode = 0;
    if (mode == DONT_CHANGE_FILE_SIZE) {
      falloc_mode = FALLOC_FL_KEEP_SIZE;
    }
    int ret;
    RETRY_ON_EINTR(ret, fallocate(fd_, falloc_mode, offset, length));
    if (ret != 0) {
      if (errno == EOPNOTSUPP) {
        KLOG_FIRST_N(WARNING, 1) << "The filesystem does not support fallocate().";
      } else if (errno == ENOSYS) {
        KLOG_FIRST_N(WARNING, 1) << "The kernel does not implement fallocate().";
      } else {
        return IOError(filename_, errno);
      }
    }
    return Status::OK();
  }

  virtual Status Truncate(uint64_t length) OVERRIDE {
    TRACE_EVENT2("io", "PosixRWFile::Truncate", "path", filename_, "length", length);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    int ret;
    RETRY_ON_EINTR(ret, ftruncate(fd_, length));
    if (ret != 0) {
      int err = errno;
      return Status::IOError(Substitute("Unable to truncate file $0", filename_),
                             Substitute("ftruncate() failed: $0", ErrnoToString(err)),
                             err);
    }
    return Status::OK();
  }

  virtual Status PunchHole(uint64_t offset, size_t length) OVERRIDE {
#if defined(__linux__)
    TRACE_EVENT1("io", "PosixRWFile::PunchHole", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();

    // KUDU-2052: xfs in el6 systems induces an fsync in the kernel whenever it
    // performs a hole punch through the fallocate() syscall, even if the file
    // range was already punched out. The older xfs-specific hole punching
    // ioctl doesn't do this, despite eventually executing the same xfs code.
    // To keep the code simple, we'll use this ioctl on any xfs system (not
    // just on el6) and fallocate() on all other filesystems.
    //
    // Note: the cast to void* here (and back to PosixRWFile*, in InitIsOnXFS)
    // is needed to avoid an undefined behavior warning from UBSAN.
    once_.Init(&InitIsOnXFS, reinterpret_cast<void*>(this));
    if (is_on_xfs_ && FLAGS_env_use_ioctl_hole_punch_on_xfs) {
      xfs_flock64_t cmd;
      memset(&cmd, 0, sizeof(cmd));
      cmd.l_start = offset;
      cmd.l_len = length;
      if (ioctl(fd_, XFS_IOC_UNRESVSP64, &cmd) < 0) {
        return IOError(filename_, errno);
      }
    } else {
      int ret;
      RETRY_ON_EINTR(ret, fallocate(
          fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
          offset, length));
      if (ret != 0) {
        return IOError(filename_, errno);
      }
    }
    return Status::OK();
#else
    return Status::NotSupported("Hole punching not supported on this platform");
#endif
  }

  virtual Status Flush(FlushMode mode, uint64_t offset, size_t length) OVERRIDE {
    TRACE_EVENT1("io", "PosixRWFile::Flush", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
#if defined(__linux__)
    int flags = SYNC_FILE_RANGE_WRITE;
    if (mode == FLUSH_SYNC) {
      flags |= SYNC_FILE_RANGE_WAIT_AFTER;
    }
    if (sync_file_range(fd_, offset, length, flags) < 0) {
      return IOError(filename_, errno);
    }
#else
    if (mode == FLUSH_SYNC && fsync(fd_) < 0) {
      return IOError(filename_, errno);
    }
#endif
    return Status::OK();
  }

  virtual Status Sync() OVERRIDE {
    TRACE_EVENT1("io", "PosixRWFile::Sync", "path", filename_);
    ThreadRestrictions::AssertIOAllowed();
    LOG_SLOW_EXECUTION(WARNING, 1000, Substitute("sync call for $0", filename())) {
      RETURN_NOT_OK(DoSync(fd_, filename_));
    }
    return Status::OK();
  }

  virtual Status Close() OVERRIDE {
    if (closed_) {
      return Status::OK();
    }
    TRACE_EVENT1("io", "PosixRWFile::Close", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status s;

    if (sync_on_close_) {
      s = Sync();
      if (!s.ok()) {
        LOG(ERROR) << "Unable to Sync " << filename_ << ": " << s.ToString();
      }
    }

    int ret;
    RETRY_ON_EINTR(ret, close(fd_));
    if (ret < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    closed_ = true;
    return s;
  }

  virtual Status Size(uint64_t* size) const OVERRIDE {
    TRACE_EVENT1("io", "PosixRWFile::Size", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    struct stat st;
    if (fstat(fd_, &st) == -1) {
      return IOError(filename_, errno);
    }
    *size = st.st_size;
    return Status::OK();
  }

  virtual Status GetExtentMap(ExtentMap* out) const OVERRIDE {
#if !defined(__linux__)
    return Status::NotSupported("GetExtentMap not supported on this platform");
#else
    TRACE_EVENT1("io", "PosixRWFile::GetExtentMap", "path", filename_);
    MAYBE_RETURN_EIO(filename_, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();

    // This allocation size is arbitrary.
    static const int kBufSize = 4096;
    uint8_t buf[kBufSize] = { 0 };

    struct fiemap* fm = reinterpret_cast<struct fiemap*>(buf);
    struct fiemap_extent* fme = &fm->fm_extents[0];
    int avail_extents_in_buffer = (kBufSize - sizeof(*fm)) / sizeof(*fme);
    bool saw_last_extent = false;
    ExtentMap extents;
    do {
      // Fetch another block of extents.
      fm->fm_length = FIEMAP_MAX_OFFSET;
      fm->fm_extent_count = avail_extents_in_buffer;
      if (ioctl(fd_, FS_IOC_FIEMAP, fm) == -1) {
        return IOError(filename_, errno);
      }

      // No extents returned, this file must have no extents.
      if (fm->fm_mapped_extents == 0) {
        break;
      }

      // Parse the extent block.
      uint64_t last_extent_end_offset;
      for (int i = 0; i < fm->fm_mapped_extents; i++) {
        if (fme[i].fe_flags & FIEMAP_EXTENT_LAST) {
          // This should really be the last extent.
          CHECK_EQ(fm->fm_mapped_extents - 1, i);

          saw_last_extent = true;
        }
        InsertOrDie(&extents, fme[i].fe_logical, fme[i].fe_length);
        VLOG(3) << Substitute("File $0 extent $1: o $2, l $3 $4",
                              filename_, i,
                              fme[i].fe_logical, fme[i].fe_length,
                              saw_last_extent ? "(final)" : "");
        last_extent_end_offset = fme[i].fe_logical + fme[i].fe_length;
        if (saw_last_extent) {
          break;
        }
      }

      fm->fm_start = last_extent_end_offset;
    } while (!saw_last_extent);

    out->swap(extents);
    return Status::OK();
#endif
  }

  bool IsEncrypted() const override {
    return encrypted_;
  }

  virtual const string& filename() const OVERRIDE {
    return filename_;
  }

  size_t GetEncryptionHeaderSize() const override {
    return encrypted_ ? kEncryptionHeaderSize : 0;
  }

 private:
  static void InitIsOnXFS(void* arg) {
    PosixRWFile* rwf = reinterpret_cast<PosixRWFile*>(arg);
    bool result;
    Status s = DoIsOnXfsFilesystem(rwf->filename_, &result);
    if (s.ok()) {
      rwf->is_on_xfs_ = result;
    } else {
      KLOG_EVERY_N_SECS(WARNING, 1) <<
          Substitute("Could not determine whether file is on xfs, assuming not: $0",
                     s.ToString());
    }
  }

  const string filename_;
  const int fd_;
  const bool sync_on_close_;

  GoogleOnceDynamic once_;
  bool is_on_xfs_;
  bool closed_;
  const bool encrypted_;
  const EncryptionHeader encryption_header_;
};

int LockOrUnlock(int fd, bool lock) {
  ThreadRestrictions::AssertIOAllowed();
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  int ret;
  RETRY_ON_EINTR(ret, fcntl(fd, F_SETLK, &f));
  return ret;
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
};

class PosixEnv : public Env {
 public:
  ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    exit(1);
  }

  virtual Status NewSequentialFile(const string& fname,
                                   unique_ptr<SequentialFile>* result) override {
    return NewSequentialFile(SequentialFileOptions(), fname, result);
  }

  virtual Status NewSequentialFile(const SequentialFileOptions& opts,
                                   const string& fname,
                                   unique_ptr<SequentialFile>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewSequentialFile", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    FILE* f;
    POINTER_RETRY_ON_EINTR(f, fopen(fname.c_str(), "r"));
    if (f == nullptr) {
      return IOError(fname, errno);
    }
    bool encrypted = opts.is_sensitive && IsEncryptionEnabled();
    EncryptionHeader header;
    if (encrypted) {
      int fd;
      RETURN_NOT_OK(DoOpen(fname, OpenMode::MUST_EXIST, &fd));
      RETURN_NOT_OK(ReadEncryptionHeader(fd, fname, &header));
      if (fseek(f, kEncryptionHeaderSize, SEEK_CUR)) {
        return IOError(fname, errno);
      }
    }
    result->reset(new PosixSequentialFile(fname, encrypted, f, header));
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const string& fname,
                                     unique_ptr<RandomAccessFile>* result) OVERRIDE {
    return NewRandomAccessFile(RandomAccessFileOptions(), fname, result);
  }

  virtual Status NewRandomAccessFile(const RandomAccessFileOptions& opts,
                                     const string& fname,
                                     unique_ptr<RandomAccessFile>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewRandomAccessFile", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    int fd;
    RETRY_ON_EINTR(fd, open(fname.c_str(), O_RDONLY));
    if (fd < 0) {
      return IOError(fname, errno);
    }
    EncryptionHeader header;
    bool encrypted = opts.is_sensitive && IsEncryptionEnabled();
    if (encrypted) {
      RETURN_NOT_OK(ReadEncryptionHeader(fd, fname, &header));
    }
    result->reset(new PosixRandomAccessFile(fname, fd,
                  encrypted, header));
    return Status::OK();
  }

  virtual Status NewWritableFile(const string& fname,
                                 unique_ptr<WritableFile>* result) OVERRIDE {
    return NewWritableFile(WritableFileOptions(), fname, result);
  }

  virtual Status NewWritableFile(const WritableFileOptions& opts,
                                 const string& fname,
                                 unique_ptr<WritableFile>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewWritableFile", "path", fname);
    int fd;
    RETURN_NOT_OK(DoOpen(fname, opts.mode, &fd));
    return InstantiateNewWritableFile(fname, fd, opts, result);
  }

  virtual Status NewTempWritableFile(const WritableFileOptions& opts,
                                     const string& name_template,
                                     string* created_filename,
                                     unique_ptr<WritableFile>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewTempWritableFile", "template", name_template);
    int fd = 0;
    string tmp_filename;
    RETURN_NOT_OK(MkTmpFile(name_template, &fd, &tmp_filename));
    RETURN_NOT_OK(InstantiateNewWritableFile(tmp_filename, fd, opts, result));
    created_filename->swap(tmp_filename);
    return Status::OK();
  }

  virtual Status NewRWFile(const string& fname,
                           unique_ptr<RWFile>* result) OVERRIDE {
    return NewRWFile(RWFileOptions(), fname, result);
  }

  virtual Status NewRWFile(const RWFileOptions& opts,
                           const string& fname,
                           unique_ptr<RWFile>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewRWFile", "path", fname);
    int fd;
    bool encrypt = opts.is_sensitive && IsEncryptionEnabled();
    uint64_t size = 0;
    if (opts.mode == MUST_EXIST) {
      RETURN_NOT_OK(GetFileSize(fname, &size));
    } else if (encrypt) {
      GetFileSize(fname, &size);
    }

    RETURN_NOT_OK(DoOpen(fname, opts.mode, &fd));
    EncryptionHeader eh;
    if (encrypt) {
      if (size >= kEncryptionHeaderSize) {
        RETURN_NOT_OK(ReadEncryptionHeader(fd, fname, &eh));
      } else {
        RETURN_NOT_OK(GenerateHeader(&eh));
        RETURN_NOT_OK(WriteEncryptionHeader(fd, fname, eh));
      }
    }
    result->reset(new PosixRWFile(fname, fd, opts.sync_on_close,
                                  encrypt, eh));
    return Status::OK();
  }

  virtual Status NewTempRWFile(const RWFileOptions& opts, const string& name_template,
                               string* created_filename, unique_ptr<RWFile>* res) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::NewTempRWFile", "template", name_template);
    int fd = 0;
    RETURN_NOT_OK(MkTmpFile(name_template, &fd, created_filename));
    bool encrypt = opts.is_sensitive && IsEncryptionEnabled();
    EncryptionHeader eh;
    if (encrypt) {
      RETURN_NOT_OK(GenerateHeader(&eh));
      RETURN_NOT_OK(WriteEncryptionHeader(fd, *created_filename, eh));
    }
    res->reset(new PosixRWFile(*created_filename, fd, opts.sync_on_close,
                               encrypt, eh));
    return Status::OK();
  }

  virtual Status NewFifo(const string& fname, unique_ptr<Fifo>* fifo) override {
    TRACE_EVENT1("io", "PosixEnv::NewFifo", "path", fname);
    int m = mkfifo(fname.c_str(), 0666);
    if (m != 0) {
      return IOError(Substitute("Error creating fifo $0", fname), errno);
    }
    fifo->reset(new PosixFifo(fname));
    return Status::OK();
  }

  virtual bool FileExists(const string& fname) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::FileExists", "path", fname);
    ThreadRestrictions::AssertIOAllowed();
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const string& dir, vector<string>* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::GetChildren", "path", dir);
    MAYBE_RETURN_EIO(dir, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == nullptr) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    // TODO: lint: Consider using readdir_r(...) instead of readdir(...) for improved thread safety.
    while ((entry = readdir(d)) != nullptr) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const string& fname) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::DeleteFile", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const string& name) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::CreateDir", "path", name);
    MAYBE_RETURN_EIO(name, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status result;
    if (mkdir(name.c_str(), 0777) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const string& name) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::DeleteDir", "path", name);
    MAYBE_RETURN_EIO(name, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  Status GetCurrentWorkingDir(string* cwd) const override {
    TRACE_EVENT0("io", "PosixEnv::GetCurrentWorkingDir");
    ThreadRestrictions::AssertIOAllowed();
    unique_ptr<char[], FreeDeleter> wd(getcwd(nullptr, 0));
    if (!wd) {
      return IOError("getcwd()", errno);
    }
    cwd->assign(wd.get());

    MAYBE_RETURN_EIO(*cwd, IOError(Env::kInjectedFailureStatusMsg, EIO));
    return Status::OK();
  }

  Status ChangeDir(const string& dest) override {
    TRACE_EVENT1("io", "PosixEnv::ChangeDir", "dest", dest);
    MAYBE_RETURN_EIO(dest, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status result;
    if (chdir(dest.c_str()) != 0) {
      result = IOError(dest, errno);
    }
    return result;
  }

  virtual Status SyncDir(const string& dirname) OVERRIDE {
    TRACE_EVENT1("io", "SyncDir", "path", dirname);
    MAYBE_RETURN_EIO(dirname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    if (FLAGS_never_fsync) return Status::OK();
    int dir_fd;
    RETRY_ON_EINTR(dir_fd, open(dirname.c_str(), O_DIRECTORY|O_RDONLY));
    if (dir_fd < 0) {
      return IOError(dirname, errno);
    }
    ScopedFdCloser fd_closer(dir_fd);
    if (fsync(dir_fd) != 0) {
      return IOError(dirname, errno);
    }
    return Status::OK();
  }

  virtual Status DeleteRecursively(const string &name) OVERRIDE {
    return Walk(
        name, POST_ORDER,
        [this](FileType type, const string& dirname, const string& basename) {
          return this->DeleteRecursivelyCb(type, dirname, basename);
        });
  }

  virtual Status GetFileSize(const string& fname, uint64_t* size) override {
    TRACE_EVENT1("io", "PosixEnv::GetFileSize", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status GetFileSizeOnDisk(const string& fname, uint64_t* size) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::GetFileSizeOnDisk", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      s = IOError(fname, errno);
    } else {
      // From stat(2):
      //
      //   The st_blocks field indicates the number of blocks allocated to
      //   the file, 512-byte units. (This may be smaller than st_size/512
      //   when the file has holes.)
      *size = sbuf.st_blocks * 512;
    }
    return s;
  }

  virtual Status GetFileSizeOnDiskRecursively(const string& root,
                                              uint64_t* bytes_used) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::GetFileSizeOnDiskRecursively", "path", root);
    uint64_t total = 0;
    RETURN_NOT_OK(Walk(
        root, PRE_ORDER,
        [this, &total](FileType type, const string& dirname, const string& basename) {
          return this->GetFileSizeOnDiskRecursivelyCb(&total, type, dirname, basename);
        }));
    *bytes_used = total;
    return Status::OK();
  }

  virtual Status GetBlockSize(const string& fname, uint64_t* block_size) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::GetBlockSize", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      s = IOError(fname, errno);
    } else {
      *block_size = sbuf.st_blksize;
    }
    return s;
  }

  virtual Status GetFileModifiedTime(const string& fname, int64_t* timestamp) override {
    TRACE_EVENT1("io", "PosixEnv::GetFileModifiedTime", "fname", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();

    struct stat s;
    if (stat(fname.c_str(), &s) != 0) {
      return IOError(fname, errno);
    }
#ifdef __APPLE__
    *timestamp = s.st_mtimespec.tv_sec * 1000000 + s.st_mtimespec.tv_nsec / 1000;
#else
    *timestamp = s.st_mtim.tv_sec * 1000000 + s.st_mtim.tv_nsec / 1000;
#endif
    return Status::OK();
  }

  // Local convenience function for safely running statvfs().
  static Status StatVfs(const string& path, struct statvfs* buf) {
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    int ret;
    RETRY_ON_EINTR(ret, statvfs(path.c_str(), buf));
    if (ret == -1) {
      return IOError(Substitute("statvfs: $0", path), errno);
    }
    return Status::OK();
  }

  virtual Status GetSpaceInfo(const string& path, SpaceInfo* space_info) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::GetSpaceInfo", "path", path);
    struct statvfs buf;
    RETURN_NOT_OK(StatVfs(path, &buf));
    space_info->capacity_bytes = buf.f_frsize * buf.f_blocks;
    space_info->free_bytes = buf.f_frsize * buf.f_bavail;
    space_info->filesystem_id = buf.f_fsid;
    return Status::OK();
  }

  virtual Status RenameFile(const string& src, const string& target) OVERRIDE {
    TRACE_EVENT2("io", "PosixEnv::RenameFile", "src", src, "dst", target);
    MAYBE_RETURN_EIO(src, IOError(Env::kInjectedFailureStatusMsg, EIO));
    MAYBE_RETURN_EIO(target, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const string& fname, FileLock** lock) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::LockFile", "path", fname);
    MAYBE_RETURN_EIO(fname, IOError(Env::kInjectedFailureStatusMsg, EIO));
    if (ShouldInject(fname, FLAGS_env_inject_lock_failure_globs)) {
      return IOError("lock " + fname, EAGAIN);
    }
    ThreadRestrictions::AssertIOAllowed();
    *lock = nullptr;
    Status result;
    int fd;
    RETRY_ON_EINTR(fd, open(fname.c_str(), O_RDWR | O_CREAT, 0666));
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      DoClose(fd);
    } else {
      auto my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) OVERRIDE {
    TRACE_EVENT0("io", "PosixEnv::UnlockFile");
    ThreadRestrictions::AssertIOAllowed();
    unique_ptr<PosixFileLock> my_lock(reinterpret_cast<PosixFileLock*>(lock));
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    DoClose(my_lock->fd_);
    return result;
  }

  virtual Status GetTestDirectory(string* result) OVERRIDE {
    string dir;
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      dir = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/kudutest-%d", static_cast<int>(geteuid()));
      dir = buf;
    }
    // Directory may already exist
    ignore_result(CreateDir(dir));
    // /tmp may be a symlink, so canonicalize the path.
    return Canonicalize(dir, result);
  }

  virtual uint64_t gettid() OVERRIDE {
    // Platform-independent thread ID.  We can't use pthread_self here,
    // because that function returns a totally opaque ID, which can't be
    // compared via normal means.
    if (thread_local_id == 0) {
      thread_local_id = Barrier_AtomicIncrement(&cur_thread_local_id_, 1);
    }
    return thread_local_id;
  }

  virtual uint64_t NowMicros() OVERRIDE {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) OVERRIDE {
    ThreadRestrictions::AssertWaitAllowed();
    SleepFor(MonoDelta::FromMicroseconds(micros));
  }

  virtual Status GetExecutablePath(string* path) OVERRIDE {
    MAYBE_RETURN_EIO("/proc/self/exe", IOError(Env::kInjectedFailureStatusMsg, EIO));
    uint32_t size = 64;
    uint32_t len = 0;
    while (true) {
      unique_ptr<char[]> buf(new char[size]);
#if defined(__linux__)
      int rc = readlink("/proc/self/exe", buf.get(), size);
      if (rc == -1) {
        return IOError("Unable to determine own executable path", errno);
      } else if (rc >= size) {
        // The buffer wasn't large enough
        size *= 2;
        continue;
      }
      len = rc;
#elif defined(__APPLE__)
      if (_NSGetExecutablePath(buf.get(), &size) != 0) {
        // The buffer wasn't large enough; 'size' has been updated.
        continue;
      }
      len = strlen(buf.get());
#else
#error Unsupported platform
#endif

      path->assign(buf.get(), len);
      break;
    }
    return Status::OK();
  }

  virtual Status IsDirectory(const string& path, bool* is_dir) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::IsDirectory", "path", path);
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    Status s;
    struct stat sbuf;
    if (stat(path.c_str(), &sbuf) != 0) {
      s = IOError(path, errno);
    } else {
      *is_dir = S_ISDIR(sbuf.st_mode);
    }
    return s;
  }

  virtual Status Walk(const string& root, DirectoryOrder order, const WalkCallback& cb) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::Walk", "path", root);
    MAYBE_RETURN_EIO(root, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    // Some sanity checks
    CHECK_NE(root, "/");
    CHECK_NE(root, "./");
    CHECK_NE(root, ".");
    CHECK_NE(root, "");

    // FTS requires a non-const copy of the name. strdup it and free() when
    // we leave scope.
    unique_ptr<char[], FreeDeleter> name_dup(strdup(root.c_str()));
    char* paths[] = { name_dup.get(), nullptr };

    // FTS_NOCHDIR is important here to make this thread-safe.
    FTS* ret;
    POINTER_RETRY_ON_EINTR(ret, fts_open(
        paths, FTS_PHYSICAL | FTS_XDEV | FTS_NOCHDIR, nullptr));
    if (ret == nullptr) {
      return IOError(root, errno);
    }
    unique_ptr<FTS, FtsCloser> tree(ret);

    FTSENT* ent = nullptr;
    bool had_errors = false;
    while ((ent = fts_read(tree.get())) != nullptr) {
      bool do_cb = false;
      FileType type = DIRECTORY_TYPE;
      switch (ent->fts_info) {
        case FTS_D:         // Directory in pre-order
          if (order == PRE_ORDER) {
            do_cb = true;
          }
          break;
        case FTS_DP:        // Directory in post-order
          if (order == POST_ORDER) {
            do_cb = true;
          }
          break;
        case FTS_F:         // A regular file
        case FTS_SL:        // A symbolic link
        case FTS_SLNONE:    // A broken symbolic link
        case FTS_DEFAULT:   // Unknown type of file
          do_cb = true;
          type = FILE_TYPE;
          break;

        case FTS_DNR:
        case FTS_ERR:
        case FTS_NS:
          LOG(WARNING) << "Unable to access file " << ent->fts_path
                       << " during walk: " << strerror(ent->fts_errno);
          LOG(WARNING) << Substitute("Unable to access file $0 during walk: $1",
                                     ent->fts_path, strerror(ent->fts_errno));
          had_errors = true;
          break;

        default:
          LOG(WARNING) << Substitute("Unable to access file $0 during walk (code $1)",
                                     ent->fts_path, ent->fts_info);
          break;
      }
      if (do_cb) {
        Status s = cb(type, DirName(ent->fts_path), ent->fts_name);
        if (PREDICT_FALSE(!s.ok())) {
          LOG(WARNING) << Substitute("Error running callback with file $0 during walk: $1",
                                     ent->fts_path, s.ToString());
          had_errors = true;
        }
      }
    }

    if (had_errors) {
      return Status::IOError(root, "One or more errors occurred");
    }
    return Status::OK();
  }

  Status Glob(const string& path_pattern, vector<string>* paths) override {
    TRACE_EVENT1("io", "PosixEnv::Glob", "path_pattern", path_pattern);
    ThreadRestrictions::AssertIOAllowed();

    glob_t result;
    auto cleanup = MakeScopedCleanup([&] { globfree(&result); });

    errno = 0;
    int ret = glob(path_pattern.c_str(), GLOB_TILDE | GLOB_ERR , NULL, &result);
    switch (ret) {
      case 0: break;
      case GLOB_NOMATCH: return Status::OK();
      case GLOB_NOSPACE: return Status::RuntimeError("glob out of memory");
      default: {
        string err = (errno != 0) ? ErrnoToString(errno) : "unknown error";
        return Status::IOError(Substitute("glob failed for $0: $1",
                                          path_pattern,
                                          err));
      }
    }

    for (size_t i = 0; i < result.gl_pathc; ++i) {
      paths->emplace_back(result.gl_pathv[i]);
    }
    return Status::OK();
  }

  virtual Status Canonicalize(const string& path, string* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::Canonicalize", "path", path);
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    unique_ptr<char[], FreeDeleter> r(realpath(path.c_str(), nullptr));
    if (!r) {
      return IOError(Substitute("Unable to canonicalize $0", path), errno);
    }
    *result = string(r.get());
    return Status::OK();
  }

  virtual Status GetTotalRAMBytes(int64_t* ram) OVERRIDE {
#if defined(__APPLE__)
    int mib[2];
    size_t length = sizeof(*ram);

    // Get the Physical memory size
    mib[0] = CTL_HW;
    mib[1] = HW_MEMSIZE;
    CHECK_ERR(sysctl(mib, 2, ram, &length, nullptr, 0)) << "sysctl CTL_HW HW_MEMSIZE failed";
#else
    struct sysinfo info;
    if (sysinfo(&info) < 0) {
      return IOError("sysinfo() failed", errno);
    }
    *ram = info.totalram;
#endif
    return Status::OK();
  }

  virtual uint64_t GetResourceLimit(ResourceLimitType t) OVERRIDE {
    static_assert(std::is_unsigned<rlim_t>::value, "rlim_t must be unsigned");
    static_assert(RLIM_INFINITY > 0, "RLIM_INFINITY must be positive");

    // There's no reason for this to ever fail.
    struct rlimit l;
    PCHECK(getrlimit(ResourceLimitTypeToUnixRlimit(t), &l) == 0);
    return l.rlim_cur;
  }

  virtual void IncreaseResourceLimit(ResourceLimitType t) OVERRIDE {
    // There's no reason for this to ever fail; any process should have
    // sufficient privilege to increase its soft limit up to the hard limit.
    //
    // This change is logged because it is process-wide.

    int rlimit_type = ResourceLimitTypeToUnixRlimit(t);
    struct rlimit l;
    PCHECK(getrlimit(rlimit_type, &l) == 0);
#if defined(__APPLE__)
    // OS X 10.11 can return RLIM_INFINITY from getrlimit, but allows rlim_cur and
    // rlim_max to be raised only as high as the value of the maxfilesperproc
    // kernel variable. Empirically, this value is 10240 across all tested macOS
    // versions. Testing on OS X 10.10 and macOS 10.12 revealed that getrlimit
    // returns the true limits (not RLIM_INFINITY), rlim_max can *not* be raised
    // (when running as non-root), and rlim_cur can only be raised as high as
    // rlim_max (this is consistent with Linux).
    // TLDR; OS X 10.11 is whack.
    if (l.rlim_max == RLIM_INFINITY) {
      uint32_t limit;
      size_t len = sizeof(limit);
      PCHECK(sysctlbyname(ResourceLimitTypeToMacosRlimit(t), &limit, &len,
                          nullptr, 0) == 0);
      // Make sure no uninitialized bits are present in the result.
      DCHECK_EQ(sizeof(limit), len);
      l.rlim_max = limit;
    }
#endif
    const char* rlimit_str = ResourceLimitTypeToString(t);
    if (l.rlim_cur < l.rlim_max) {
      LOG(INFO) << Substitute("Raising this process' $0 limit from $1 to $2",
                              rlimit_str, l.rlim_cur, l.rlim_max);
      l.rlim_cur = l.rlim_max;
      PCHECK(setrlimit(rlimit_type, &l) == 0);
    } else {
      LOG(INFO) << Substitute("Not raising this process' $0 limit of $1; it "
          "is already as high as it can go", rlimit_str, l.rlim_cur);
    }
  }

  virtual Status IsOnExtFilesystem(const string& path, bool* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::IsOnExtFilesystem", "path", path);
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();

#ifdef __APPLE__
    *result = false;
#else
    struct statfs buf;
    int ret;
    RETRY_ON_EINTR(ret, statfs(path.c_str(), &buf));
    if (ret == -1) {
      return IOError(Substitute("statfs: $0", path), errno);
    }
    *result = (buf.f_type == EXT4_SUPER_MAGIC);
#endif
    return Status::OK();
  }

  virtual Status IsOnXfsFilesystem(const string& path, bool* result) OVERRIDE {
    TRACE_EVENT1("io", "PosixEnv::IsOnXfsFilesystem", "path", path);
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    ThreadRestrictions::AssertIOAllowed();
    return DoIsOnXfsFilesystem(path, result);
  }

  virtual string GetKernelRelease() OVERRIDE {
    // There's no reason for this to ever fail.
    struct utsname u;
    PCHECK(uname(&u) == 0);
    return string(u.release);
  }

  Status EnsureFileModeAdheresToUmask(const string& path) override {
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    struct stat s;
    if (stat(path.c_str(), &s) != 0) {
      return IOError("stat", errno);
    }
    CHECK_NE(g_parsed_umask, -1);
    if (s.st_mode & g_parsed_umask) {
      uint32_t old_perms = s.st_mode & ACCESSPERMS;
      uint32_t new_perms = old_perms & ~g_parsed_umask;
      LOG(WARNING) << "Path " << path << " has permissions "
                   << StringPrintf("%03o", old_perms)
                   << " which are less restrictive than current umask value "
                   << StringPrintf("%03o", g_parsed_umask)
                   << ": resetting permissions to "
                   << StringPrintf("%03o", new_perms);
      if (chmod(path.c_str(), new_perms) != 0) {
        return IOError("chmod", errno);
      }
    }
    return Status::OK();
  }

  Status IsFileWorldReadable(const string& path, bool* result) override {
    ThreadRestrictions::AssertIOAllowed();
    TRACE_EVENT1("io", "PosixEnv::IsFileWorldReadable", "path", path);
    MAYBE_RETURN_EIO(path, IOError(Env::kInjectedFailureStatusMsg, EIO));
    struct stat s;
    if (stat(path.c_str(), &s) != 0) {
      return IOError("stat", errno);
    }
    *result = (s.st_mode & S_IROTH) != 0;
    return Status::OK();
  }

  virtual Status CreateSymLink(const string& src, const string& dst) override {
    ThreadRestrictions::AssertIOAllowed();
    TRACE_EVENT2("io", "PosixEnv::CreateSymLink", "src", src, "dst", dst);
    MAYBE_RETURN_EIO(dst, IOError(Env::kInjectedFailureStatusMsg, EIO));
    Status result;
    if (symlink(src.c_str(), dst.c_str()) != 0) {
      result = IOError(dst, errno);
    }
    return result;
  }

  bool IsEncryptionEnabled() const override { return FLAGS_encrypt_data_at_rest; }

 private:
  // unique_ptr Deleter implementation for fts_close
  struct FtsCloser {
    void operator()(FTS *fts) const {
      if (fts) {
        int err;
        RETRY_ON_EINTR(err, fts_close(fts));
        if (PREDICT_FALSE(err != 0)) {
          PLOG(WARNING) << "Failed to close fts";
        }
      }
    }
  };

  Status MkTmpFile(const string& name_template, int* fd, string* created_filename) {
    ThreadRestrictions::AssertIOAllowed();
    unique_ptr<char[]> fname(new char[name_template.size() + 1]);
    ::snprintf(fname.get(), name_template.size() + 1, "%s", name_template.c_str());
    MAYBE_RETURN_EIO(fname.get(), IOError(Env::kInjectedFailureStatusMsg, EIO));
    int created_fd = mkstemp(fname.get());
    if (created_fd < 0) {
      return IOError(Substitute("Call to mkstemp() failed on name template $0", name_template),
                     errno);
    }
    // mkstemp defaults to making files with permissions 0600. But, if the
    // user configured a more permissive umask, then we ensure that the
    // resulting file gets the desired (wider) permissions.
    uint32_t new_perms = 0666 & ~g_parsed_umask;
    if (new_perms != 0600) {
      CHECK_ERR(fchmod(created_fd, new_perms));
    }
    *fd = created_fd;
    *created_filename = fname.get();
    return Status::OK();
  }

  Status InstantiateNewWritableFile(const string& fname,
                                    int fd,
                                    const WritableFileOptions& opts,
                                    unique_ptr<WritableFile>* result) {
    uint64_t file_size = 0;
    Status s = GetFileSize(fname, &file_size);
    if (opts.mode == MUST_EXIST) {
      RETURN_NOT_OK(s);
    }
    bool encrypt = opts.is_sensitive && IsEncryptionEnabled();
    EncryptionHeader eh;
    if (encrypt) {
      if (file_size < kEncryptionHeaderSize) {
        RETURN_NOT_OK(GenerateHeader(&eh));
        RETURN_NOT_OK(WriteEncryptionHeader(fd, fname, eh));
        file_size = kEncryptionHeaderSize;
      } else {
        RETURN_NOT_OK(ReadEncryptionHeader(fd, fname, &eh));
      }
    }
    result->reset(new PosixWritableFile(fname, fd, file_size, opts.sync_on_close,
                                        encrypt, eh));

    return Status::OK();
  }

  Status DeleteRecursivelyCb(FileType type, const string& dirname, const string& basename) {
    string full_path = JoinPathSegments(dirname, basename);
    Status s;
    switch (type) {
      case FILE_TYPE:
        s = DeleteFile(full_path);
        WARN_NOT_OK(s, "Could not delete file");
        return s;
      case DIRECTORY_TYPE:
        s = DeleteDir(full_path);
        WARN_NOT_OK(s, "Could not delete directory");
        return s;
      default:
        LOG(FATAL) << "Unknown file type: " << type;
        return Status::OK();
    }
  }

  size_t GetEncryptionHeaderSize() const override {
    return IsEncryptionEnabled() ? kEncryptionHeaderSize : 0;
  }

  Status GetFileSizeOnDiskRecursivelyCb(uint64_t* bytes_used,
                                        FileType type,
                                        const string& dirname,
                                        const string& basename) {
    uint64_t file_bytes_used = 0;
    switch (type) {
      case Env::FILE_TYPE:
        RETURN_NOT_OK(GetFileSizeOnDisk(
            JoinPathSegments(dirname, basename), &file_bytes_used));
        *bytes_used += file_bytes_used;
        break;
      case Env::DIRECTORY_TYPE:
        // Ignore directory space consumption as it varies from filesystem to
        // filesystem.
        break;
      default:
        LOG(FATAL) << "Unknown file type: " << type;
    }
    return Status::OK();
  }
};

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

std::ostream& operator<<(std::ostream& o, Env::ResourceLimitType t) {
  return o << ResourceLimitTypeToString(t);
}

}  // namespace kudu
