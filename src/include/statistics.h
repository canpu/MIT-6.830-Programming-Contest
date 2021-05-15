#pragma once
#include <vector>

// histogram of unsigned integers; each interval is left-inclusive and right-exclusive
// TODO: add get_entries function for bulk importing
class Histogram {
    protected:
        std::size_t interval_width;
        std::vector<size_t> interval_count; // number of records in each interval

    public:
        Histogram() = default;
        Histogram(unsigned int interval_width);
        Histogram(unsigned int interval_width, unsigned int estimated_histogram_max);
        ~Histogram() {}

        inline std::size_t get_number_of_intervals() {
            return interval_count.size();
        }

        inline unsigned int get_histogram_min() {
            return 0;
        }

        inline unsigned int get_histogram_max() {
            if (get_number_of_intervals() > 0)
                return get_number_of_intervals() * interval_width - 1;
            else
                return 0;
        }

        inline unsigned int get_interval_index(unsigned int entry) {
            return entry / interval_width;
        }

        void add_entry(unsigned int v);

        std::size_t get_total_number_of_records();

        std::size_t get_number_of_records_geq(unsigned int threshold);

        std::size_t get_number_of_records_gt(unsigned int threshold);

        std::size_t get_number_of_records_leq(unsigned int threshold);

        std::size_t get_number_of_records_lt(unsigned int threshold);

        std::size_t get_number_of_records_geq_leq(unsigned int low, unsigned int high);

        std::size_t get_number_of_records_geq_lt(unsigned int low, unsigned int high);

        std::size_t get_number_of_records_gt_leq(unsigned int low, unsigned int high);

        std::size_t get_number_of_records_gt_lt(unsigned int low, unsigned int high);
};

