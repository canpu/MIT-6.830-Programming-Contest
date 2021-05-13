#include "statistics.h"
#include "omp.h"
#include <limits.h>

using namespace::std;

Histogram::Histogram(uint64_t interval_width) {
    this->interval_width = interval_width;
    this->interval_count = vector<size_t> (0, 0);
}

Histogram::Histogram(uint64_t interval_width, uint64_t estimated_histogram_max) {
    this->interval_width = interval_width;
    this->interval_count = vector<size_t> (get_interval_index(estimated_histogram_max) + 1, 0);
}

void Histogram::add_entry(uint64_t entry) {
    size_t i_index = get_interval_index(entry);
    if (i_index + 1 > get_number_of_intervals()) {
        interval_count.resize(i_index + 1, 0);
    } else {
        interval_count[i_index] += 1;
    }
}

size_t Histogram::get_total_number_of_records() {
    size_t count = 0;
    size_t number_of_intervals = get_number_of_intervals();
    #pragma omp parallel for reduction (+: count)
    for (size_t i = 0; i < number_of_intervals; ++i) {
        count += interval_count[i];
    }
    return count;
}

size_t Histogram::get_number_of_records_geq(uint64_t threshold) {
    if (threshold > get_histogram_max())
        return 0;
    size_t number_of_intervals = get_number_of_intervals();
    size_t i_index = get_interval_index(threshold);
    size_t sum = 0;

    // parallelization: summation of counts
    #pragma omp parallel for reduction (+: sum)
    for (size_t i = i_index + 1; i < number_of_intervals; ++i) {
        sum += interval_count[i];
    }

    // estimate and add the count inside the boundary interval
    size_t i_entry = interval_width - threshold % interval_width;
    sum += static_cast<size_t>(static_cast<float>(interval_count[i_index])
                    * static_cast<float>(i_entry) / static_cast<float>(interval_width));

    return sum;
}

size_t Histogram::get_number_of_records_gt(uint64_t threshold) {
    if (threshold == UINT_MAX)
        return 0;
    else
        return this->get_number_of_records_geq(threshold + 1);
}

size_t Histogram::get_number_of_records_leq(uint64_t threshold) {
    if (threshold >= get_histogram_max()) {
        return get_total_number_of_records();
    }
    size_t number_of_intervals = get_number_of_intervals();
    size_t i_index = get_interval_index(threshold);
    size_t sum = 0;

    // parallelization: summation of counts
    #pragma omp parallel for reduction (+: sum)
    for (size_t i = 0; i < i_index; ++i) {
        sum += interval_count[i];
    }

    // estimate and add the count inside the boundary interval
    size_t i_entry = threshold % interval_width + 1;
    sum += static_cast<size_t>(static_cast<float>(interval_count[i_index])
                    * static_cast<float>(i_entry) / static_cast<float>(interval_width));

    return sum;
}

size_t Histogram::get_number_of_records_lt(uint64_t threshold) {
    if (threshold == 0)
        return 0;
    else
        return get_number_of_records_leq(threshold - 1);
}

size_t Histogram::get_number_of_records_geq_leq(uint64_t low, uint64_t high) {

    if (low > high)
        return 0;
    else if (low == 0)
        return get_number_of_records_leq(high);
    else if (high >= get_histogram_max())
        return get_number_of_records_geq(low);

    size_t number_of_intervals = get_number_of_intervals();
    size_t left_i_index = get_interval_index(low);
    size_t right_i_index = get_interval_index(high);
    size_t sum = 0;

    // parallelization: summation of counts
    #pragma omp parallel for reduction (+: sum)
    for (size_t i = left_i_index + 1; i < right_i_index; ++i) {
        sum += interval_count[i];
    }

    // estimate and add the count inside the boundary interval
    float float_width = static_cast<float>(interval_width);
    if (left_i_index == right_i_index) {
        size_t i_entry = (high % interval_width) - (low % interval_width) + 1;
        sum += static_cast<size_t>(static_cast<float>(interval_count[left_i_index])
            * static_cast<float>(i_entry) / float_width);
    } else {
        size_t left_width = interval_width - (low % interval_width);
        size_t right_width = high % interval_width + 1;
        sum += static_cast<size_t>(static_cast<float>(interval_count[left_i_index])
            * static_cast<float>(left_width) / float_width) +
            static_cast<size_t>(static_cast<float>(interval_count[right_i_index])
            * static_cast<float>(right_width) / float_width);
    }

    return sum;
}

size_t Histogram::get_number_of_records_geq_lt(uint64_t low, uint64_t high) {
    if (high == 0)
        return 0;
    else
        return get_number_of_records_geq_leq(low, high - 1);
}

size_t Histogram::get_number_of_records_gt_leq(uint64_t low, uint64_t high) {
    if (low == UINT_MAX)
        return 0;
    else
        return get_number_of_records_geq_leq(low + 1, high);
}

size_t Histogram::get_number_of_records_gt_lt(uint64_t low, uint64_t high) {
    if (low == UINT_MAX || high == 0)
        return 0;
    else
        return get_number_of_records_geq_leq(low + 1, high - 1);
}