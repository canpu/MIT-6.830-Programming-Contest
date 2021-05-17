#include "utils.h"

#include <iostream>

static double filter_time = 0.0;
static double join_prep_time = 0.0, self_join_prep_time = 0.0;
static double join_materialization_time = 0.0, join_probing_time = 0.0, join_build_time = 0.0;
static double self_join_materialization_time = 0.0, self_join_probing_time = 0.0;
static double check_sum_time = 0.0;
static double total_time = 0.0;

using namespace::std;

// Create a dummy column
static void createColumn(std::vector<uint64_t *> &columns,
                         uint64_t num_tuples) {
    auto col = new uint64_t[num_tuples];
    columns.push_back(col);
    for (unsigned i = 0; i < num_tuples; ++i) {
        col[i] = i;
    }
}

// Create a dummy relation
Relation Utils::createRelation(uint64_t size, uint64_t num_columns) {
    std::vector<uint64_t *> columns;
    for (unsigned i = 0; i < num_columns; ++i) {
        createColumn(columns, size);
    }
    return Relation(size, move(columns));
}

// Store a relation in all formats
void Utils::storeRelation(std::ofstream &out, Relation &r, unsigned i) {
    auto base_name = "r" + std::to_string(i);
    r.storeRelation(base_name);
    r.storeRelationCSV(base_name);
    r.dumpSQL(base_name, i);
    std::cout << base_name << "\n";
    out << base_name << "\n";
}



// Timer
double * get_filter_time() {
    return &filter_time;
}

double * get_total_time() {
    return &total_time;
}

double * get_self_join_prep_time() {
    return &self_join_prep_time;
}


double * get_self_join_probing_time() {
    return &self_join_probing_time;
}


double * get_self_join_materialization_time() {
    return &self_join_materialization_time;
}


double * get_join_prep_time() {
    return &join_prep_time;
}


double * get_join_probing_time() {
    return &join_probing_time;
}


double * get_join_build_time() {
    return &join_build_time;
}


double * get_join_materialization_time() {
    return &join_materialization_time;
}


double * get_checksum_time() {
    return &check_sum_time;
}

void reset_time() {
    total_time = 0.0;
    filter_time = 0.0;
    self_join_prep_time = 0.0;
    self_join_probing_time = 0.0;
    self_join_materialization_time = 0.0;
    join_prep_time = 0.0;
    join_probing_time = 0.0;
    join_build_time = 0.0;
    join_materialization_time = 0.0;
    check_sum_time = 0.0;
}

void display_time() {
    double join_time = join_prep_time + join_probing_time + join_materialization_time + join_build_time;
    double self_join_time = self_join_prep_time + self_join_probing_time + self_join_materialization_time;
    double tracked_time = filter_time + self_join_time + join_time + check_sum_time;
    cerr << endl;
    cerr << "Total time = " << total_time << " sec." << endl;
    cerr << "Tracked time = " << tracked_time << " sec." << endl;
    cerr << "    FilterScan time = " << filter_time << " sec." << endl;
    cerr << "    SelfJoin time = " << self_join_time  << " sec." << endl;
    cerr << "        Preparation time = " << self_join_prep_time << " sec." << endl;
    cerr << "        Probing time = " << self_join_probing_time << " sec." << endl;
    cerr << "        Materialization time = " << self_join_materialization_time << " sec." << endl;
    cerr << "    Join time = " << join_time << " sec." << endl;
    cerr << "        Preparation time = " << join_prep_time << " sec." << endl;
    cerr << "        Building time = " << join_build_time << " sec." << endl;
    cerr << "        Probing time = " << join_probing_time << " sec." << endl;
    cerr << "        Materialization time = " << join_materialization_time << " sec." << endl;
    cerr << "    Checksum time = " << check_sum_time << " sec." << endl;
    cerr << "Untracked time = " << total_time - tracked_time << " sec." << endl;
}

