#pragma once
#include <vector>
#include <stdint.h>


// histogram of uint64_tegers; each interval is left-inclusive and right-exclusive
// TODO: add get_entries function for bulk importing
class Histogram {
    protected:
        std::size_t interval_width;
        std::vector<size_t> interval_count; // number of records in each interval

    public:
        Histogram() = default;
        Histogram(uint64_t interval_width);
        Histogram(uint64_t interval_width, uint64_t estimated_histogram_max);
        ~Histogram() {}

        inline std::size_t get_number_of_intervals() {
            return interval_count.size();
        }

        inline uint64_t get_histogram_min() {
            return 0;
        }

        inline uint64_t get_histogram_max() {
            if (get_number_of_intervals() > 0)
                return get_number_of_intervals() * interval_width - 1;
            else
                return 0;
        }

        inline uint64_t get_interval_index(uint64_t entry) {
            return entry / interval_width;
        }

        inline std::size_t get_number_of_records_eq(uint64_t entry) {
            return interval_count[get_interval_index(entry)] / interval_width;
        }

        void add_entry(uint64_t v);

        std::size_t get_similarity(Histogram other);

        std::size_t get_total_number_of_records();

        std::size_t get_number_of_records_geq(uint64_t threshold);

        std::size_t get_number_of_records_gt(uint64_t threshold);

        std::size_t get_number_of_records_leq(uint64_t threshold);

        std::size_t get_number_of_records_lt(uint64_t threshold);

        std::size_t get_number_of_records_geq_leq(uint64_t low, uint64_t high);

        std::size_t get_number_of_records_geq_lt(uint64_t low, uint64_t high);

        std::size_t get_number_of_records_gt_leq(uint64_t low, uint64_t high);

        std::size_t get_number_of_records_gt_lt(uint64_t low, uint64_t high);
};

