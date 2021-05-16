#include "operators.h"
#include <omp.h>
#include <set>
#include <utility>

#include <cassert>
#include <iostream>

#define NUM_THREADS 24
#define DEPTH_WORTHY_PARALLELIZATION 3
#define RESERVE_FACTOR 4

static double filter_time = 0.0;
static double join_materialization_time = 0.0, join_probing_time = 0.0, join_build_time = 0.0;
static double self_join_materialization_time = 0.0, self_join_probing_time = 0.0;

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
    double begin_time = omp_get_wtime(), end_time;

    size_t input_data_size = relation_.size();
    size_t num_cols = input_data_.size();

    uint64_t size_per_thread;
    uint64_t num_threads;
    if (input_data_size < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION) {
        num_threads = 1;
        size_per_thread = input_data_size;
    } else
        num_threads = NUM_THREADS;
    size_per_thread = (input_data_size / num_threads) + (input_data_size % num_threads != 0);
    vector<vector<size_t>> thread_selected_ids(num_threads);
    vector<size_t> thread_result_sizes = vector<size_t> (num_threads, 0);

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t tid = omp_get_thread_num();
        thread_selected_ids[tid] = vector<size_t> ();
        thread_selected_ids[tid].reserve(size_per_thread);

        uint64_t col_offset = tid * size_per_thread;
        uint64_t ii;

        for (uint64_t i = col_offset; i < col_offset + size_per_thread && i < input_data_size; ++i) {
            bool pass = true;
            for (auto &f : filters_) {
            pass &= applyFilter(i, f);
                if (!pass) break;
            }
            if (pass) {
                thread_selected_ids[tid].push_back(i);
            }
        }
        thread_result_sizes[tid] = thread_selected_ids[tid].size();
    }

    // Reduction
    vector<size_t> thread_cum_sizes = vector<size_t> (num_threads + 1, 0);
    result_size_ = 0;
    for (uint64_t t = 0; t < num_threads; ++t) {
        thread_cum_sizes[t+1] = thread_cum_sizes[t] + thread_result_sizes[t];
        result_size_ += thread_result_sizes[t];
    }

    // Materialization
    for (size_t c = 0; c < num_cols; ++c) {
        tmp_results_[c].reserve(result_size_);
    }

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t tid = omp_get_thread_num();

        vector<size_t> &selected = thread_selected_ids[tid];
        size_t t_size = thread_result_sizes[tid];
        size_t cur_ind = thread_cum_sizes[tid];

        for (uint64_t i = 0; i < t_size; ++i) {
            size_t id = selected[i];
            for (unsigned cId = 0; cId < num_cols; ++cId) {
                tmp_results_[cId][cur_ind] = input_data_[cId][id];
            }
            cur_ind++;
        }
    }

    end_time = omp_get_wtime();
    filter_time += (end_time - begin_time);
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

    size_t left_num_cols = copy_left_data_.size();
    for (unsigned cId = 0; cId < left_num_cols; ++cId)
        tmp_results_[cId].push_back(copy_left_data_[cId][left_id]);

    size_t right_num_cols = copy_right_data_.size();
    for (unsigned cId = 0; cId < right_num_cols; ++cId)
        tmp_results_[left_num_cols+cId].push_back(copy_right_data_[cId][right_id]);

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

    uint64_t left_input_size = left_->result_size();

    // Build phase
    double begin_time = omp_get_wtime(), end_time;

    auto left_key_column = left_input_data[left_col_id];

    hash_table_.reserve(left_input_size * RESERVE_FACTOR);
    for (uint64_t i = 0, limit = i + left_input_size; i != limit; ++i) {
        hash_table_.emplace(left_key_column[i], i);
    }

    end_time = omp_get_wtime();
    join_build_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Probe phase
    size_t left_num_cols = copy_left_data_.size();
    size_t right_num_cols = copy_right_data_.size();
    size_t tot_num_cols = left_num_cols + right_num_cols;
    auto right_key_column = right_input_data[right_col_id];
    uint64_t right_input_size = right_->result_size();

    uint64_t size_per_thread;
    uint64_t num_threads;
    if (right_input_size < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION) {
        num_threads = 1;
        size_per_thread = right_input_size;
    } else {
        num_threads = NUM_THREADS;
        size_per_thread = (right_input_size / num_threads) + (right_input_size % num_threads != 0);
    }
    vector<vector<size_t>> thread_selected_ids(num_threads);
    vector<size_t> thread_result_sizes = vector<size_t> (num_threads, 0);

    vector<uint64_t> left_selected, right_selected;
    vector<uint64_t> thread_left_selected[NUM_THREADS];
    vector<uint64_t> thread_right_selected[NUM_THREADS];

    #pragma omp parallel num_threads(NUM_THREADS)
    {
        uint64_t thread_id = omp_get_thread_num();
        thread_left_selected[thread_id].reserve(right_input_size * RESERVE_FACTOR);
        thread_right_selected[thread_id].reserve(right_input_size * RESERVE_FACTOR);
        uint64_t start_ind = thread_id * size_per_thread;
        uint64_t end_ind = (thread_id + 1) * size_per_thread;
        if (end_ind > right_input_size)
            end_ind = right_input_size;

        for (uint64_t right_id = start_ind; right_id < end_ind; ++right_id) {
            auto right_key_val = right_key_column[right_id];
            for (uint64_t t = 0; t < 1; ++t) {
                auto range = hash_table_.equal_range(right_key_val);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    uint64_t left_id = iter->second;
                    thread_left_selected[thread_id].push_back(left_id);
                    thread_right_selected[thread_id].push_back(right_id);
                }
            }
        }
    }

    for (uint64_t i = 0; i < NUM_THREADS; ++i) {
        left_selected.insert(left_selected.end(), thread_left_selected[i].begin(), thread_left_selected[i].end());
        right_selected.insert(right_selected.end(), thread_right_selected[i].begin(), thread_right_selected[i].end());
        result_size_ += thread_right_selected[i].size();
    }

////    #pragma omp parallel for
//    for (uint64_t right_id = 0; right_id < right_input_size; ++right_id) {
//        auto right_key_val = right_key_column[right_id];
//        auto range = hash_table_.equal_range(right_key_val);
//        vector<uint64_t> left_selected_private, right_selected_private;
//        for (auto iter = range.first; iter != range.second; ++iter) {
//            uint64_t left_id = iter->second;
////            #pragma omp critical
//            {
//                left_selected.push_back(left_id);
//                right_selected.push_back(right_id);
//                ++result_size_;
//            }
//        }
//    }

    end_time = omp_get_wtime();
    join_probing_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Materialization phase
    #pragma omp parallel num_threads(NUM_THREADS)
    {
        uint64_t num_threads = omp_get_num_threads();
        uint64_t thread_id = omp_get_thread_num();

        for (uint64_t col = thread_id; col < left_num_cols; col += num_threads) {
            tmp_results_[col].reserve(result_size_);
            uint64_t *col_data = tmp_results_[col].data();
            unsigned id;
            for (size_t i = 0; i < result_size_; ++i) {
                id = left_selected[i];
                col_data[i] = copy_left_data_[col][id];
            }
        }

        for (uint64_t col = thread_id; col < right_num_cols; col += num_threads) {
            uint64_t global_col = left_num_cols + col;
            tmp_results_[global_col].reserve(result_size_);
            uint64_t *col_data = tmp_results_[left_num_cols + col].data();
            unsigned id;
            for (size_t i = 0; i < result_size_; ++i) {
                id = right_selected[i];
                col_data[i] = copy_right_data_[col][id];
            }
        }
    }

    end_time = omp_get_wtime();
    join_materialization_time += (end_time - begin_time);
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

    input_->require(p_info_.left);
    input_->require(p_info_.right);
    input_->run();

    input_data_ = input_->getResults();

    for (auto &iu : required_IUs_) {
        auto id = input_->resolve(iu);
        copy_data_.emplace_back(input_data_[id]);
        select_to_result_col_id_.emplace(iu, copy_data_.size() - 1);
    }

    size_t tot_num_cols = copy_data_.size();
    uint64_t input_data_size = input_->result_size();
    auto left_col_id = input_->resolve(p_info_.left);
    auto right_col_id = input_->resolve(p_info_.right);
    auto left_col = input_data_[left_col_id];
    auto right_col = input_data_[right_col_id];

    // Probing
    double begin_time = omp_get_wtime(), end_time;

    uint64_t size_per_thread;
    uint64_t num_threads;
    if (input_data_size < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION) {
        num_threads = 1;
        size_per_thread = input_data_size;
    } else
        num_threads = NUM_THREADS;
    size_per_thread = (input_data_size / num_threads) + (input_data_size % num_threads != 0);
    vector<vector<size_t>> thread_selected_ids(num_threads);
    vector<size_t> thread_result_sizes = vector<size_t> (num_threads, 0);

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t thread_id = omp_get_thread_num();
        thread_selected_ids[thread_id] = vector<size_t> ();
        thread_selected_ids[thread_id].reserve(size_per_thread);

        uint64_t start_ind = thread_id * size_per_thread;
        uint64_t end_ind = start_ind + size_per_thread;
        if (end_ind > input_data_size)
            end_ind = input_data_size;

        for (uint64_t i = start_ind; i < end_ind; ++i) {
            if (left_col[i] == right_col[i]) {
                thread_selected_ids[thread_id].push_back(i);
            }
        }
        thread_result_sizes[thread_id] = thread_selected_ids[thread_id].size();
    }

    // Reduction
    vector<size_t> thread_cum_sizes = vector<size_t> (num_threads + 1, 0);
    result_size_ = 0;
    for (uint64_t t = 0; t < num_threads; ++t) {
        thread_cum_sizes[t+1] = thread_cum_sizes[t] + thread_result_sizes[t];
        result_size_ += thread_result_sizes[t];
    }

    end_time = omp_get_wtime();
    self_join_probing_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Materialization
    for (size_t c = 0; c < tot_num_cols; ++c) {
        tmp_results_[c].reserve(result_size_);
    }

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t tid = omp_get_thread_num();

        vector<size_t> &selected = thread_selected_ids[tid];
        size_t t_size = thread_result_sizes[tid];
        size_t cur_ind = thread_cum_sizes[tid];

        for (uint64_t i = 0; i < t_size; ++i) {
            size_t id = selected[i];
            for (unsigned cId = 0; cId < tot_num_cols; ++cId) {
                tmp_results_[cId][cur_ind] = copy_data_[cId][id];
            }
            cur_ind++;
        }
    }

    end_time = omp_get_wtime();
    self_join_materialization_time += (end_time - begin_time);
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

// Timer
void reset_time() {
    filter_time = 0.0;
    self_join_probing_time = 0.0;
    self_join_materialization_time = 0.0;
    join_probing_time = 0.0;
    join_build_time = 0.0;
    join_materialization_time = 0.0;
}

void display_time() {
    cerr << "FilterScan time = " << filter_time << " sec." << endl;
    cerr << "SelfJoin time = " << self_join_probing_time + self_join_materialization_time  << " sec." << endl;
    cerr << "\tProbing time = " << self_join_probing_time << " sec." << endl;
    cerr << "\tMaterialization time = " << self_join_materialization_time << " sec." << endl;
    cerr << "Join time = " << join_probing_time + join_materialization_time + join_build_time << " sec." << endl;
    cerr << "\tBuilding time = " << join_build_time << " sec." << endl;
    cerr << "\tProbing time = " << join_probing_time << " sec." << endl;
    cerr << "\tMaterialization time = " << join_materialization_time << " sec." << endl;
}
