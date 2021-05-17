#pragma once

#include <fstream>

#include "relation.h"

class Utils {
    public:
        /// Create a dummy relation
        static Relation createRelation(uint64_t size, uint64_t num_columns);

        /// Store a relation in all formats
        static void storeRelation(std::ofstream &out, Relation &r, unsigned i);
};

void get_time_ptrs(double *join_prep_time, double *join_build_time, double *join_probing_time, double *join_materialization_time,
                   double *self_join_prep_time, double *self_join_probing_time, double *self_join_materialization_time,
                   double *check_sum_time, double *filter_time);

// Timer
void reset_time();

void display_time();

double * get_filter_time();
double * get_self_join_prep_time();
double * get_self_join_probing_time();
double * get_self_join_materialization_time();
double * get_join_prep_time();
double * get_join_probing_time();
double * get_join_build_time();
double * get_join_materialization_time();
double * get_checksum_time();

