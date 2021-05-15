#include "operators.h"
#include "omp.h"
#include <set>
#include <utility>

#include <cassert>
#include <iostream>

using namespace::std;

// Get materialized results
std::vector<uint64_t *> Operator::getResults() {
    size_t data_size = tmp_results_.size();
    std::vector<uint64_t *> result_vector(data_size);
    for (size_t i = 0; i < data_size; ++i) {
        result_vector[i] = tmp_results_[i].data();
    }
    return result_vector;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info) {
    if (info.binding != relation_binding_)
        return false;
    assert(info.col_id < relation_.columns().size());
    result_columns_.push_back(relation_.columns()[info.col_id]);
    select_to_result_col_id_[info] = result_columns_.size() - 1;
    return true;
}

// Run
void Scan::run() {
    // Nothing to do
    result_size_ = relation_.size();
}

// Get materialized results
std::vector<uint64_t *> Scan::getResults() {
    return result_columns_;
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info) {
    if (info.binding != relation_binding_)
        return false;
    assert(info.col_id < relation_.columns().size());
    if (select_to_result_col_id_.find(info) == select_to_result_col_id_.end()) {
        // Add to results
        input_data_.push_back(relation_.columns()[info.col_id]);
        tmp_results_.emplace_back();
        unsigned colId = tmp_results_.size() - 1;
        select_to_result_col_id_[info] = colId;
    }
    return true;
}

// Copy to result
void FilterScan::copy2Result(uint64_t id) {
    size_t input_data_size = input_data_.size();
    for (unsigned cId = 0; cId < input_data_size; ++cId)
        tmp_results_[cId].push_back(input_data_[cId][id]);
    ++result_size_;
}

// Apply filter
bool FilterScan::applyFilter(uint64_t i, FilterInfo &f) {
    auto compare_col = relation_.columns()[f.filter_column.col_id];
    auto constant = f.constant;
    switch (f.comparison) {
        case FilterInfo::Comparison::Equal:return compare_col[i] == constant;
        case FilterInfo::Comparison::Greater:return compare_col[i] > constant;
        case FilterInfo::Comparison::Less:return compare_col[i] < constant;
    };
    return false;
}

// Run
void FilterScan::run() {
    size_t relation_size = relation_.size();

    for (size_t i = 0; i < relation_size; ++i) {
        bool pass = true;
        for (auto &f : filters_) {
            pass &= applyFilter(i, f);
            if (!pass) break;
        }
        if (pass)
            copy2Result(i);
    }
}

// Require a column and add it to results
bool Join::require(SelectInfo info) {
    if (requested_columns_.count(info) == 0) {
        bool success = false;
        if (left_->require(info)) {
            requested_columns_left_.emplace_back(info);
            success = true;
        } else if (right_->require(info)) {
            success = true;
            requested_columns_right_.emplace_back(info);
        }
        if (!success)
            return false;

        tmp_results_.emplace_back();
        requested_columns_.emplace(info);
    }
    return true;
}

// Copy to result
void Join::copy2Result(uint64_t left_id, uint64_t right_id) {
    unsigned rel_col_id = 0;

    size_t left_data_size = copy_left_data_.size();
    for (unsigned cId = 0; cId < left_data_size; ++cId)
        tmp_results_[cId].push_back(copy_left_data_[cId][left_id]);

    size_t right_data_size = copy_right_data_.size();
    for (unsigned cId = 0; cId < right_data_size; ++cId)
        tmp_results_[left_data_size+cId].push_back(copy_right_data_[cId][right_id]);
    
    ++result_size_;
}

// Run
void Join::run() {
    left_->require(p_info_.left);
    right_->require(p_info_.right);
    left_->run();
    right_->run();

    // Use smaller input_ for build
    if (left_->result_size() > right_->result_size()) {
        std::swap(left_, right_);
        std::swap(p_info_.left, p_info_.right);
        std::swap(requested_columns_left_, requested_columns_right_);
    }

    auto left_input_data = left_->getResults();
    auto right_input_data = right_->getResults();

    // Resolve the input_ columns_
    unsigned res_col_id = 0;
    for (auto &info : requested_columns_left_) {
        copy_left_data_.push_back(left_input_data[left_->resolve(info)]);
        select_to_result_col_id_[info] = res_col_id++;
    }
    for (auto &info : requested_columns_right_) {
        copy_right_data_.push_back(right_input_data[right_->resolve(info)]);
        select_to_result_col_id_[info] = res_col_id++;
    }

    auto left_col_id = left_->resolve(p_info_.left);
    auto right_col_id = right_->resolve(p_info_.right);

    // Build phase
    auto left_key_column = left_input_data[left_col_id];
    hash_table_.reserve(left_->result_size() * 2);
    for (uint64_t i = 0, limit = i + left_->result_size(); i != limit; ++i) {
        hash_table_.emplace(left_key_column[i], i);
    }

    // Probe phase
    size_t left_data_size = copy_left_data_.size();
    size_t right_data_size = copy_right_data_.size();
    auto right_key_column = right_input_data[right_col_id];
    uint64_t right_input_size = right_->result_size();
    std::vector<std::pair<uint64_t, uint64_t>> pairs;
    vector<uint64_t> t1, t2;
    for (uint64_t right_id = 0; right_id < right_input_size; ++right_id) {
        auto right_key_val = right_key_column[right_id];
        auto range = hash_table_.equal_range(right_key_val);
        for (auto iter = range.first; iter != range.second; ++iter) {
            uint64_t left_id = iter->second;
            t1.push_back(left_id);
            t2.push_back(right_id);
            ++result_size_;
        }
    }

    // Materialization phase
//    for (unsigned cId = 0; cId < left_data_size + right_data_size; ++cId)
//        tmp_results_[cId].reserve(result_size_);
//
//    #pragma omp parallel for
//    for (size_t i = 0; i < result_size_; ++i) {
//        auto left_id = t1[i];
//        auto right_id = t2[i];
//
//        for (unsigned cId = 0; cId < left_data_size; ++cId)
//            tmp_results_[cId][i] = copy_left_data_[cId][left_id];
//
//        for (unsigned cId = 0; cId < right_data_size; ++cId)
//            tmp_results_[left_data_size+cId][i] = copy_right_data_[cId][right_id];
//    }



    #pragma omp parallel for
    for (size_t col = 0; col < left_data_size; ++col) {
        tmp_results_[col].reserve(result_size_);
        for (size_t i = 0; i < result_size_; ++i) {
            uint64_t left_id = t1[i];
            uint64_t right_id = t2[i];
            tmp_results_[col][i] = copy_left_data_[col][left_id];
        }
    }

    #pragma omp parallel for
    for (size_t col = 0; col < right_data_size; ++col) {
        tmp_results_[left_data_size + col].reserve(result_size_);
        for (size_t i = 0; i < result_size_; ++i) {
            uint64_t left_id = t1[i];
            uint64_t right_id = t2[i];
            tmp_results_[left_data_size + col][i] = copy_right_data_[col][right_id];
        }
    }
}

// Copy to result
void SelfJoin::copy2Result(uint64_t id) {
    size_t data_size = copy_data_.size();
    for (unsigned cId = 0; cId < data_size; ++cId)
        tmp_results_[cId].push_back(copy_data_[cId][id]);
    ++result_size_;
}

// Require a column and add it to results
bool SelfJoin::require(SelectInfo info) {
    if (required_IUs_.count(info))
        return true;
    if (input_->require(info)) {
        tmp_results_.emplace_back();
        required_IUs_.emplace(info);
        return true;
    }
    return false;
}

// Run
void SelfJoin::run() {
    // TODO: edit late materialization
    // TODO: edit to use hash
    // strategy: first reserve the max space, then compare
    // strategy: divice c1=c2 and c1!=c2

    input_->require(p_info_.left);
    input_->require(p_info_.right);
    input_->run();

    input_data_ = input_->getResults();

    for (auto &iu : required_IUs_) {
        auto id = input_->resolve(iu);
        copy_data_.emplace_back(input_data_[id]);
        select_to_result_col_id_.emplace(iu, copy_data_.size() - 1);
    }

    auto left_col_id = input_->resolve(p_info_.left);
    auto right_col_id = input_->resolve(p_info_.right);
    auto left_col = input_data_[left_col_id];
    auto right_col = input_data_[right_col_id];

//    std::unordered_map<uint64_t, std::set<unsigned>> left_hash;
//    for (unsigned row_index = 0; row_index < input_->result_size(); ++row_index) {
//        uint64_t val = left_col[row_index];
//        if (left_hash.find(val) == left_hash.end()) {
//            std::set<unsigned> tmp_set;
//            tmp_set.emplace(row_index);
//            left_hash.emplace(val, tmp_set);
//        } else {
//            left_hash[val].emplace(row_index);
//        }
//    }

//    size_t num_cols = copy_data_.size();
//    result_size_ = 0;
//    for (unsigned row_index = 0; row_index < input_size; ++row_index) {
//        uint64_t val = right_col[row_index];
//        if (left_hash.find(val) != left_hash.end()) {
//            for (unsigned left_index: left_hash[val]) {
//                for (unsigned cId = 0; cId < num_cols; ++cId)
//                    tmp_results_[cId].push_back(copy_data_[cId][row_index]);
//                ++result_size_;
//            }
//        }
//    }


    for (uint64_t i = 0; i < input_->result_size(); ++i) {
        if (left_col[i] == right_col[i]) {
            size_t data_size = copy_data_.size();
            for (unsigned cId = 0; cId < data_size; ++cId)
                tmp_results_[cId].push_back(copy_data_[cId][i]);
            ++result_size_;
        }
    }
}

// Run
void Checksum::run() {
    for (auto &sInfo : col_info_) {
        input_->require(sInfo);
    }
    input_->run();
    auto results = input_->getResults();

    for (auto &sInfo : col_info_) {
        auto col_id = input_->resolve(sInfo);
        auto result_col = results[col_id];
        uint64_t sum = 0;
        result_size_ = input_->result_size();

        for (auto iter = result_col, limit = iter + input_->result_size();
            iter != limit;
            ++iter)
            sum += *iter;
        check_sums_.push_back(sum);
    }
}

