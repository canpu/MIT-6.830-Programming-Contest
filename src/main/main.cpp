#include <iostream>

#include "joiner.h"
#include "parser.h"
#include <vector>
#include "utils.h"
#include "omp.h"

double * total_time = get_total_time();

int main(int argc, char *argv[]) {
    Joiner joiner;

    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    // Preparation phase (not timed)
    // Build histograms, indexes,...
    // TOOD: iterate over all relations and columns in joiner, and build maps for them
    // TODO: iterate over all relations and columns in joiner, and build histograms for them
    for (const Relation &relation : joiner.relations()) {

    }

    reset_time();
    double start = omp_get_wtime();

    QueryInfo i;
    while (getline(std::cin, line)) {
        if (line == "F") continue; // End of a batch
        i.parseQuery(line);
        std::cout << joiner.join(i);
    }

    *total_time = (omp_get_wtime() - start);
    display_time();

    return 0;
}
