#pragma once

#include "benchmark/gitclone/config.h"
#include "benchmark/gitclone/constants.h"
#include "benchmark/gitclone/schema.h"
#include "benchmark/utils/misc.h"
#include "common/rand.h"

#include "share_headers/config.h"
#include "share_headers/csv.h"
#include "share_headers/logger.h"

#include <iostream>
#include <string>

#define MAX_INROW_SIZE leanstore::schema::MAX_INROW_SIZE

/**
 * @brief For your information, please check `README.md` file
 */
namespace gitclone {

template <template <typename> class AdapterType, typename SystemRelation, typename TemplateRelation,
          typename ObjectRelation>
struct GitCloneWorkload {
  AdapterType<SystemRelation> system_files;
  AdapterType<TemplateRelation> template_files;
  AdapterType<ObjectRelation> objects;
  const bool enable_blob_rep;

  // Workload characteristics
  std::vector<Operator> operators;

  template <typename... Params>
  explicit GitCloneWorkload(bool enable_blob_rep, Params &&...params)
      : system_files(AdapterType<SystemRelation>(std::forward<Params>(params)...)),
        template_files(AdapterType<TemplateRelation>(std::forward<Params>(params)...)),
        objects(AdapterType<ObjectRelation>(std::forward<Params>(params)...)),
        enable_blob_rep(enable_blob_rep) {
    // Only support single-thread
    Ensure(FLAGS_worker_count == 1);

    // Workload characteristics
    io::CSVReader<4> in(FLAGS_git_workload_config_path.c_str());
    in.read_header(io::ignore_extra_column, "op", "fd", "size", "flags");
    std::string op_type;
    int file_desc = 0;
    uint64_t size = 0;
    int flags     = 0;
    while (in.read_row(op_type, file_desc, size, flags)) {
      operators.emplace_back(Operator::type_resolver[op_type], file_desc, size, flags);
    }
  }

  // -------------------------------------------------------------------------------------
  /** Shared utilities by both impl */
  auto CountObjects() -> uint64_t { return objects.Count(); }

  void MainPhaseImplementation(const std::function<void(const Operator &op, uint64_t &next_key,
                                                        std::unique_ptr<uint8_t[], FreeDelete> &payload)> &op_exec) {
    auto next_key                                  = TEMPLATE_FILE_INFO.size() + 1;
    std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;

    for (auto &op : operators) {
      Ensure(op.op != Operator::Type::READ);
      // Ignore the below two cases (total of 1.3% of main traces)
      if (op.file_desc == 100) {
        Ensure(op.op == Operator::Type::FSTAT);
        continue;
      }
      if (op.file_desc == 2) {
        Ensure(op.op == Operator::Type::WRITE);
        continue;
      }
      Ensure(op.file_desc == 4);
      op_exec(op, next_key, payload);
    }
  }

  // -------------------------------------------------------------------------------------
  void LoadDataWithoutBlobRep() {
    SetupFilesWithoutBlobRep(SYSTEM_FILE_INFO, system_files);
    SetupFilesWithoutBlobRep(TEMPLATE_FILE_INFO, template_files);
  }

  template <std::ranges::random_access_range R, typename Relation>
  void SetupFilesWithoutBlobRep(const R &file_range, AdapterType<Relation> &relation) {
    std::unique_ptr<uint8_t[]> payload;

    for (auto key = 0UL; key < file_range.size(); key++) {
      auto &[file, payload_size] = file_range[key];
      payload.reset(new uint8_t[sizeof(Varchar<0>) + payload_size]);
      auto object            = reinterpret_cast<Relation *>(payload.get());
      object->payload.length = payload_size;
      RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(object->payload.data), 100UL, payload_size);

      relation.Insert({key + 1}, *object);
    }
  }

  void InitializationPhaseWithoutBlobRep() {
    Ensure(!enable_blob_rep);
    std::unique_ptr<uint8_t[], FreeDelete> payload;

    // Read system files
    for (auto key = 0UL; key < SYSTEM_FILE_INFO.size(); key++) {
      payload    = nullptr;
      auto found = system_files.LookUp({key + 1}, [&](const SystemRelation &rec) {
        payload.reset(reinterpret_cast<uint8_t *>(malloc(rec.payload.length)));
        std::memcpy(payload.get(), rec.payload.data, rec.payload.length);
      });
      Ensure(found);
    }

    // Copy template files into the main object relation
    for (auto key = 0UL; key < TEMPLATE_FILE_INFO.size(); key++) {
      payload    = nullptr;
      auto found = template_files.LookUp({key + 1}, [&](const TemplateRelation &rec) {
        payload.reset(reinterpret_cast<uint8_t *>(malloc(rec.PayloadSize())));
        std::memcpy(payload.get(), &rec, rec.PayloadSize());
      });
      Ensure(found);
      // Don't construct a new FileRelation() here to prevent extra copy
      objects.Insert({key + 1}, *reinterpret_cast<ObjectRelation *>(payload.get()));
    }
  }

  void MainPhaseWithoutBlobRep() {
    Ensure(!enable_blob_rep);

    MainPhaseImplementation(
      [&](const Operator &op, uint64_t &next_key, std::unique_ptr<uint8_t[], FreeDelete> &payload) {
        switch (op.op) {
          case Operator::Type::OPEN: payload = nullptr; break;
          case Operator::Type::WRITE: {
            payload.reset(reinterpret_cast<uint8_t *>(malloc(sizeof(Varchar<0>) + op.size)));
            auto object            = reinterpret_cast<ObjectRelation *>(payload.get());
            object->payload.length = op.size;
            RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(object->payload.data), 100UL, op.size);
            // Don't construct a new FileRelation() here to prevent extra copy
            objects.Insert({next_key}, *object);
            break;
          }
          case Operator::Type::FSTAT: {
            objects.FileStat({next_key});
            break;
          }
          case Operator::Type::CLOSE: next_key++; break;
          default: UnreachableCode();
        }
      });
  }

  // -------------------------------------------------------------------------------------
  void LoadInitialData() {
    SetupExistingFiles(SYSTEM_FILE_INFO, system_files);
    SetupExistingFiles(TEMPLATE_FILE_INFO, template_files);
  }

  template <std::ranges::random_access_range R, typename Relation>
  void SetupExistingFiles(const R &file_range, AdapterType<Relation> &relation) {
    uint8_t tmp[Relation::MAX_RECORD_SIZE];
    Relation *record;

    for (auto key = 0UL; key < file_range.size(); key++) {
      auto &[file, payload_size] = file_range[key];
      std::unique_ptr<uint8_t[], FreeDelete> payload(reinterpret_cast<uint8_t *>(malloc(payload_size)));
      RandomGenerator::GetRandRepetitiveString(payload.get(), 100UL, payload_size);

      if (enable_blob_rep && payload_size > MAX_INROW_SIZE) {
        auto blob_rep = relation.RegisterBlob({payload.get(), payload_size}, {}, false);
        // NOLINTNEXTLINE
        record = new (tmp) Relation{0, blob_rep};
      } else {
        // NOLINTNEXTLINE
        record = new (tmp) Relation{payload_size, {payload.get(), payload_size}};
      }
      relation.Insert({key + 1}, *record);
    }
  }

  /**
   * @brief Read file `key` from file relation `relation`, and store the content into `out_tuple`,
   *  or `out_payload` if the file is not fit into a single row
   */
  template <typename Relation>
  void ReadFilePayload(const uint64_t key, AdapterType<Relation> &relation, bool &out_is_offrow, uint8_t *out_tuple,
                       std::unique_ptr<uint8_t[]> &out_payload, uint64_t &out_payload_size) {
    auto found = relation.LookUp({key}, [&](const Relation &rec) {
      out_is_offrow    = rec.IsOffrowRecord();
      out_payload_size = rec.PayloadSize();
      std::memcpy(out_tuple, const_cast<Relation &>(rec).payload, rec.PayloadSize());
    });
    Ensure(found);
    if (out_is_offrow) {
      relation.LoadBlob(
        out_tuple,
        [&](std::span<const u8> blob_data) {
          out_payload.reset(reinterpret_cast<uint8_t *>(malloc(blob_data.size())));
          out_payload_size = blob_data.size();
          std::memcpy(out_payload.get(), blob_data.data(), blob_data.size());
        },
        false);
    }
  }

  void NewObject(bool is_offrow, uint64_t key, uint8_t *payload, size_t payload_size) {
    Ensure(enable_blob_rep);
    uint8_t tmp[ObjectRelation::MAX_RECORD_SIZE];
    ObjectRelation *record;

    if (is_offrow) {
      Ensure(payload_size > MAX_INROW_SIZE);
      auto blob_rep = objects.RegisterBlob({payload, payload_size}, {}, false);
      // NOLINTNEXTLINE
      record = new (tmp) ObjectRelation{0, blob_rep};
    } else {
      Ensure(payload_size <= MAX_INROW_SIZE);
      // NOLINTNEXTLINE
      record = new (tmp) ObjectRelation{payload_size, {payload, payload_size}};
    }

    objects.Insert({key + 1}, *record);
  }

  void InitializationPhase() {
    Ensure(enable_blob_rep);
    bool is_offrow        = false;
    uint64_t payload_size = 0;
    uint8_t tuple[MAX_INROW_SIZE];

    // Read system files
    for (auto key = 0UL; key < SYSTEM_FILE_INFO.size(); key++) {
      std::unique_ptr<uint8_t[]> payload = nullptr;
      ReadFilePayload(key + 1, system_files, is_offrow, tuple, payload, payload_size);
    }

    // Copy template files into the main object relation
    for (auto key = 0UL; key < TEMPLATE_FILE_INFO.size(); key++) {
      std::unique_ptr<uint8_t[]> payload = nullptr;
      ReadFilePayload(key + 1, template_files, is_offrow, tuple, payload, payload_size);
      NewObject(is_offrow, key + 1, (is_offrow) ? payload.get() : &tuple[0], payload_size);
    }
  }

  void MainPhase() {
    Ensure(enable_blob_rep);
    uint8_t blob_rep[MAX_INROW_SIZE];

    MainPhaseImplementation([&](const Operator &op, uint64_t &next_key,
                                std::unique_ptr<uint8_t[], FreeDelete> &payload) {
      switch (op.op) {
        case Operator::Type::OPEN: payload = nullptr; break;
        case Operator::Type::WRITE: {
          payload.reset(reinterpret_cast<uint8_t *>(malloc(op.size)));
          RandomGenerator::GetRandRepetitiveString(payload.get(), 100UL, op.size);
          auto is_offrow = op.size > MAX_INROW_SIZE;
          NewObject(is_offrow, next_key, payload.get(), op.size);
          break;
        }
        case Operator::Type::FSTAT: {
          [[maybe_unused]] auto found =
            objects.LookUp({next_key}, [&](const auto &rec) { std::memcpy(blob_rep, rec.payload, rec.PayloadSize()); });
          /**
           * @brief Found -> There is a file + blob_rep contains metadata
           * Not found -> No file
           */
          break;
        }
        case Operator::Type::CLOSE: next_key++; break;
        default: UnreachableCode();
      }
    });
  }
};

}  // namespace gitclone
