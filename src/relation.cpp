#include "relation.h"

#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include "omp.h"

#include "statistics.h"

// Stores a relation into a binary file
void Relation::storeRelation(const std::string &file_name) {
    std::ofstream out_file;
    out_file.open(file_name, std::ios::out | std::ios::binary);
    out_file.write((char *) &size_, sizeof(size_));
    auto numColumns = columns_.size();
    out_file.write((char *) &numColumns, sizeof(size_t));
    for (auto c : columns_) {
        out_file.write((char *) c, size_ * sizeof(uint64_t));
    }
    out_file.close();
}

// Creates histogram
const std::vector<Histogram> Relation::createHistogram() const {
  std::vector<Histogram> histograms;

  for (unsigned i = 0; i < columns_.size(); i++) {
    uint64_t min = columns_[i][0];
    uint64_t max = columns_[i][0];

    for (unsigned randomIndex = 0; randomIndex < size_/10; randomIndex++) {
      unsigned random = columns_[i][rand() % size_];

      if (random < min) min = random;
      if (random > max) max = random;
    }

    Histogram histogram = Histogram((max-min)/10, max);

    for(int tupleIndex = 0; tupleIndex < size_; tupleIndex++) {
      histogram.add_entry(columns_[i][tupleIndex]);
    }
    
    histograms.push_back(histogram);
  }

  return histograms;
}

// Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
void Relation::storeRelationCSV(const std::string &file_name) {
    std::ofstream out_file;
    out_file.open(file_name + ".tbl", std::ios::out);
    for (uint64_t i = 0; i < size_; ++i) {
        for (auto &c : columns_) {
            out_file << c[i] << '|';
        }
        out_file << "\n";
    }
}

// Dump SQL: Create and load table (PostgreSQL)
void Relation::dumpSQL(const std::string &file_name, unsigned relation_id) {
    std::ofstream out_file;
    out_file.open(file_name + ".sql", std::ios::out);
    // Create table statement
    out_file << "CREATE TABLE r" << relation_id << " (";
    for (unsigned cId = 0; cId < columns_.size(); ++cId) {
        out_file << "c" << cId << " bigint"
                 << (cId < columns_.size() - 1 ? "," : "");
    }
    out_file << ");\n";
    // Load from csv statement
    out_file << "copy r" << relation_id << " from 'r" << relation_id
             << ".tbl' delimiter '|';\n";
}

void Relation::loadRelation(const char *file_name) {
    int fd = open(file_name, O_RDONLY);
    if (fd == -1) {
        std::cerr << "cannot open " << file_name << std::endl;
        throw;
    }

    // Obtain file size_
    struct stat sb{};
    if (fstat(fd, &sb) == -1)
        std::cerr << "fstat\n";

    auto length = sb.st_size;

    char *addr = static_cast<char *>(mmap(nullptr,
                                          length,
                                          PROT_READ,
                                          MAP_PRIVATE,
                                          fd,
                                          0u));
    if (addr == MAP_FAILED) {
        std::cerr << "cannot mmap " << file_name << " of length " << length
                  << std::endl;
        throw;
    }

    if (length < 16) {
        std::cerr << "relation_ file " << file_name
                  << " does not contain a valid header"
                  << std::endl;
        throw;
    }

    this->size_ = *reinterpret_cast<uint64_t *>(addr);
    addr += sizeof(size_);
    auto numColumns = *reinterpret_cast<size_t *>(addr);
    addr += sizeof(size_t);
    this->columns_.resize(numColumns);
    #pragma omp parallel for
    for (unsigned i = 0; i < numColumns; ++i) {
        char *current = addr + size_ * sizeof(uint64_t) * i;
        this->columns_[i] = (reinterpret_cast<uint64_t *>(current));
    }
}

// Constructor that loads relation_ from disk
Relation::Relation(const char *file_name) : owns_memory_(false), size_(0) {
    loadRelation(file_name);
}

//void Relation::buildHashMaps() {
//    loadRelation(file_name);
//    size_t num_columns = columns_.size();
//    maps.reserve(num_columns);
//    #pragma omp parallel for
//    for (size_t c = 0; c < num_columns; ++c) {
//        // TODO: build hash maps for column c
//    }
//}

// Construct hash map for a specific column
//void Relation::buildHashMap(unsigned col_id) {
//    unordered_map<uint64_t, set<unsigned>> map;
//    for (unsigned t = 0; t < size_; ++t) {
//        uint64_t val = columns_[col_id][t];
//        if (map.find(val) == map.end()) {
//            set<unsigned> indices = set<unsigned> ();
//            indices.emplace(t);
//            map.insert(indices);
//        } else {
//            map[val].emplace(t);
//        }
//    }
//    maps.emplace(col_id, map);
//}

// Construct hash map for a specific column
//const unordered_map<uint64_t, set<unsigned>>& Relation::getHashMap(unsigned col_id) {
//    if (maps.find(col_id) == map.end())
//        buildHashMap(col_id);
//    return maps[col_id];
//}

// Destructor
Relation::~Relation() {
    if (owns_memory_) {
        size_t num_columns = columns_.size();
        for (size_t i = 0; i < num_columns; ++i)
            delete[] columns_[i];
    }
}

