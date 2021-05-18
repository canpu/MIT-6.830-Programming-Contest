#include "operators.h"
#include <omp.h>
#include <set>
#include <utility>
#include "utils.h"

#include <cassert>
#include <iostream>

#define NUM_THREADS 48
#define DEPTH_WORTHY_PARALLELIZATION 1
#define RESERVE_FACTOR 2

using namespace::std;

double *join_prep_time = get_join_prep_time(),
       *join_build_time = get_join_build_time(),
       *join_probing_time = get_join_probing_time(),
       *join_materialization_time = get_join_materialization_time(),
       *self_join_prep_time = get_self_join_prep_time(),
       *self_join_probing_time = get_self_join_probing_time(),
       *self_join_materialization_time = get_self_join_materialization_time(),
       *check_sum_time = get_checksum_time(),
       *filter_time = get_filter_time();

// Get materialized results
std::vector<uint64_t *> Operator::getResults() {
    size_t num_cols = tmp_results_.size();
    std::vector<uint64_t *> result_vector(num_cols);
    if (num_cols < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION)
        for (size_t i = 0; i < num_cols; ++i) {
            result_vector[i] = tmp_results_[i].data();
        }
    else {
        size_t num_cols_per_thread = (num_cols / NUM_THREADS) + (num_cols % NUM_THREADS);
        #pragma omp parallel num_threads(NUM_THREADS)
        {
            size_t tid = omp_get_thread_num();
            size_t start_ind = num_cols_per_thread * tid;
            size_t end_ind = start_ind + num_cols_per_thread;
            if (end_ind > num_cols) end_ind = num_cols;
            for (size_t i = start_ind; i < end_ind; ++i)
                result_vector[i] = tmp_results_[i].data();
        }
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

        uint64_t start_ind = tid * size_per_thread;
        uint64_t end_ind = start_ind + size_per_thread;
        if (end_ind > input_data_size) end_ind = input_data_size;

        uint64_t size = 0;
        bool pass;

        for (uint64_t i = start_ind; i < end_ind; ++i) {
            pass = true;
            for (auto &f : filters_) {
            pass &= applyFilter(i, f);
                if (!pass) break;
            }
            if (pass) {
                thread_selected_ids[tid][size] = i;
                ++size;
            }
        }
        thread_result_sizes[tid] = size;
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
    *filter_time += (end_time - begin_time);
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

// Swap
void Join::swap() {
    // Use smaller input_ for build
    if (left_->result_size() > right_->result_size()) {
        std::swap(left_, right_);
        std::swap(p_info_.left, p_info_.right);
        std::swap(requested_columns_left_, requested_columns_right_);
    }
}

// Run
void Join::run() {

    left_->require(p_info_.left);
    right_->require(p_info_.right);
    left_->run();
    right_->run();

    // Preparation phase
    double begin_time = omp_get_wtime(), end_time;
    this->swap();

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
    auto left_key_column = left_input_data[left_col_id];
    size_t left_num_cols = copy_left_data_.size();
    size_t right_num_cols = copy_right_data_.size();
    size_t tot_num_cols = left_num_cols + right_num_cols;
    auto right_key_column = right_input_data[right_col_id];
    uint64_t right_input_size = right_->result_size();

    uint64_t right_size_per_thread;
    uint64_t num_threads;
    if (right_input_size < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION) {
        num_threads = 1;
        right_size_per_thread = right_input_size;
    } else {
        num_threads = NUM_THREADS;
        right_size_per_thread = (right_input_size / num_threads) + (right_input_size % num_threads != 0);
    }
    uint64_t left_size_per_thread = (left_input_size / num_threads) + (left_input_size % num_threads != 0);

    end_time = omp_get_wtime();
    *join_prep_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Build phase
    vector<HT> hash_maps(num_threads);
    vector<uint64_t> rem(left_input_size);
    vector<uint64_t> quot(left_input_size);
    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t tid = omp_get_thread_num();
        uint64_t start = left_size_per_thread * tid;
        uint64_t end = start + left_size_per_thread;
        if (end > left_input_size) end = left_input_size;
        for (uint64_t i = start; i < end; ++i) {
            rem[i] = left_key_column[i] % num_threads;
            quot[i] = left_key_column[i] / num_threads;
        }

        #pragma omp barrier
        hash_maps[tid].reserve(left_size_per_thread * RESERVE_FACTOR);

        for (uint64_t i = 0; i < left_input_size; ++i) {
            if (rem[i] == tid) {
                hash_maps[tid].emplace(quot[i], i);
            }
        }
    }

    end_time = omp_get_wtime();
    *join_build_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Probe phase
    vector<vector<size_t>> thread_selected_ids(num_threads);
    vector<size_t> thread_result_sizes = vector<size_t> (num_threads, 0);

    vector<vector<uint64_t>> thread_left_selected(num_threads);
    vector<vector<uint64_t>> thread_right_selected(num_threads);
    vector<uint64_t> thread_sizes(num_threads);

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t thread_id = omp_get_thread_num();
        thread_left_selected[thread_id].reserve(right_size_per_thread * RESERVE_FACTOR);
        thread_right_selected[thread_id].reserve(right_size_per_thread * RESERVE_FACTOR);
        uint64_t start_ind = thread_id * right_size_per_thread;
        uint64_t end_ind = (thread_id + 1) * right_size_per_thread;
        if (end_ind > right_input_size) end_ind = right_input_size;

        for (uint64_t right_id = start_ind; right_id < end_ind; ++right_id) {
            auto right_key_val = right_key_column[right_id];
            auto range = hash_maps[right_key_val % num_threads].equal_range(right_key_val / num_threads);
            for (auto iter = range.first; iter != range.second; ++iter) {
                uint64_t left_id = iter->second;
                thread_left_selected[thread_id].push_back(left_id);
                thread_right_selected[thread_id].push_back(right_id);
            }
        }
        thread_sizes[thread_id] = thread_right_selected[thread_id].size();
    }

    // Reduction
    vector<size_t> thread_cum_sizes = vector<size_t> (num_threads + 1, 0);
    result_size_ = 0;
    for (uint64_t t = 0; t < num_threads; ++t) {
        result_size_ += thread_sizes[t];
        thread_cum_sizes[t+1] = thread_cum_sizes[t] + thread_sizes[t];
    }

    end_time = omp_get_wtime();
    *join_probing_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Materialization phase
    for (size_t c = 0; c < tot_num_cols; ++c) {
        tmp_results_[c].reserve(result_size_);
    }

    #pragma omp parallel num_threads(num_threads)
    {
        uint64_t thread_id = omp_get_thread_num();
        vector<size_t> &left_ids = thread_left_selected[thread_id];
        vector<size_t> &right_ids = thread_right_selected[thread_id];
        size_t t_size = thread_sizes[thread_id];
        size_t cur_ind = thread_cum_sizes[thread_id];

        for (uint64_t i = 0; i < t_size; ++i) {
            size_t left_id = left_ids[i];
            size_t right_id = right_ids[i];
            for (unsigned cId = 0; cId < left_num_cols; ++cId) {
                tmp_results_[cId][cur_ind] = copy_left_data_[cId][left_id];
            }
            for (unsigned cId = 0; cId < right_num_cols; ++cId) {
                tmp_results_[left_num_cols+cId][cur_ind] = copy_right_data_[cId][right_id];
            }
            cur_ind++;
        }
    }

    end_time = omp_get_wtime();
    *join_materialization_time += (end_time - begin_time);
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

    double begin_time = omp_get_wtime(), end_time;

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

    end_time = omp_get_wtime();
    *self_join_prep_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Single-Thread
    if (input_data_size < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION) {
        // Probing
        uint64_t *selected = new uint64_t [input_data_size];
        result_size_ = 0;

        for (uint64_t i = 0; i < input_data_size; ++i) {
            if (left_col[i] == right_col[i]) {
                selected[result_size_] = i;
                result_size_++;
            }
        }

        end_time = omp_get_wtime();
        *self_join_probing_time += (end_time - begin_time);
        begin_time = omp_get_wtime();

        // Materialization
        uint64_t **col_ptrs = new uint64_t* [tot_num_cols];
        for (size_t cId = 0; cId < tot_num_cols; ++cId) {
            vector<uint64_t> &col = tmp_results_[cId];
            col.reserve(result_size_);
            col_ptrs[cId] = col.data();
        }

        for (uint64_t i = 0; i < result_size_; ++i) {
            uint64_t id = selected[i];
            for (unsigned cId = 0; cId < tot_num_cols; ++cId)
                col_ptrs[cId][i] = copy_data_[cId][id];
        }

        delete [] selected;
        delete [] col_ptrs;
        return;
    }

    // Multi-thread
    // Probing
    uint64_t size_per_thread = (input_data_size / NUM_THREADS) + (input_data_size % NUM_THREADS != 0);
    size_t *thread_selected_ids[NUM_THREADS];
    size_t thread_result_sizes[NUM_THREADS];

    #pragma omp parallel num_threads(NUM_THREADS)
    {
        uint64_t thread_id = omp_get_thread_num();
        thread_selected_ids[thread_id] = new size_t[size_per_thread];
        size_t *selected = thread_selected_ids[thread_id];
        size_t thread_size = 0;

        uint64_t start_ind = thread_id * size_per_thread;
        uint64_t end_ind = start_ind + size_per_thread;
        if (end_ind > input_data_size) end_ind = input_data_size;

        for (uint64_t i = start_ind; i < end_ind; ++i) {
            if (left_col[i] == right_col[i]) {
                selected[thread_size] = i;
                ++thread_size;
            }
        }
        thread_result_sizes[thread_id] = thread_size;
    }

    // Reduction
    size_t thread_cum_sizes [NUM_THREADS + 1] = {0};
    result_size_ = 0;
    for (uint64_t t = 0; t < NUM_THREADS; ++t) {
        thread_cum_sizes[t+1] = thread_cum_sizes[t] + thread_result_sizes[t];
        result_size_ += thread_result_sizes[t];
    }

    end_time = omp_get_wtime();
    *self_join_probing_time += (end_time - begin_time);
    begin_time = omp_get_wtime();

    // Merge
    for (size_t c = 0; c < tot_num_cols; ++c) {
        tmp_results_[c].reserve(result_size_);
    }

    uint64_t **col_ptrs = new uint64_t* [tot_num_cols];
    for (size_t cId = 0; cId < tot_num_cols; ++cId) {
        vector<uint64_t> &col = tmp_results_[cId];
        col.reserve(result_size_);
        col_ptrs[cId] = col.data();
    }

    #pragma omp parallel num_threads(NUM_THREADS)
    {
        uint64_t tid = omp_get_thread_num();
        size_t *selected = thread_selected_ids[tid];
        size_t t_size = thread_result_sizes[tid];
        size_t cur_ind = thread_cum_sizes[tid];

        for (uint64_t i = 0; i < t_size; ++i) {
            uint64_t id = selected[i];
            for (unsigned cId = 0; cId < tot_num_cols; ++cId)
                col_ptrs[cId][cur_ind] = copy_data_[cId][id];
            cur_ind++;
        }
        delete [] thread_selected_ids[tid];
    }

    end_time = omp_get_wtime();
    *self_join_materialization_time += (end_time - begin_time);

    delete [] col_ptrs;
}

// Run
void Checksum::run() {
    for (auto &sInfo : col_info_) {
        input_->require(sInfo);
    }
    input_->run();

    double begin_time = omp_get_wtime(), end_time;

    auto results = input_->getResults();
    result_size_ = input_->result_size();

    auto old_num_cols = check_sums_.size();
    auto num_cols = col_info_.size();
    check_sums_.resize(old_num_cols + num_cols);

    uint64_t num_threads = result_size_ < NUM_THREADS * DEPTH_WORTHY_PARALLELIZATION ? 1 : NUM_THREADS;

    #pragma omp parallel num_threads(num_threads)
    {
        for (size_t c = omp_get_thread_num(); c < num_cols; c += num_threads) {
            const SelectInfo &sInfo = col_info_[c];
            auto col_id = input_->resolve(sInfo);
            uint64_t *result_col = results[col_id];
            uint64_t sum = 0;
            uint64_t *last = result_col + input_->result_size();

            for (uint64_t *iter = result_col; iter != last; ++iter)
                sum += *iter;
            check_sums_[old_num_cols + c] = (sum);
        }
    }

    end_time = omp_get_wtime();
    *check_sum_time += (end_time - begin_time);
}