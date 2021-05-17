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

    // Build histograms
    reset_time();
    double start = omp_get_wtime();
    
    auto relations = &joiner.relations();

    std::vector<std::vector<Histogram>> relationHistograms;
    std::vector<size_t> relationCardinalities;

    for (unsigned i = 0; i < relations->size(); i++) {
      std::vector<Histogram> histogram = relations->at(i).createHistogram();
      relationHistograms.push_back(histogram);
      relationCardinalities.push_back(relations->at(i).size());
    }
    
    // Find most restrictive filters

    QueryInfo i;

    while (getline(std::cin, line)) {
      std::vector<size_t> filterSizes = relationCardinalities;

      if (line == "F") continue; // End of a batch
      i.parseQuery(line);
      std::vector<FilterInfo> filters = i.filters();

      // Predict size of filter for every filter in query and populate filterSizes
      for (FilterInfo filterInfo: filters) {
        SelectInfo selectInfo = filterInfo.filter_column;
        Histogram histogram = relationHistograms[selectInfo.rel_id][selectInfo.col_id];
        uint64_t constant = filterInfo.constant;

        size_t size;

        switch (filterInfo.comparison) {
          case '<':
            size = histogram.get_number_of_records_lt(constant);
            break;
          case '>':
            size = histogram.get_number_of_records_gt(constant);
            break;
          case '=': 
            size = histogram.get_number_of_records_eq(constant);
            break;
        }

        filterSizes[selectInfo.rel_id] = size;
      }

      // Creates optimized predicate order vector
      // Orders based on max of estimated cardinalities of predicate
      std::vector<PredicateInfo> predicateOrder;
      std::vector<unsigned> estimatedCardinalities;
      
      for (PredicateInfo predicateInfo: i.predicates()) {
        unsigned leftId = predicateInfo.left.rel_id;
        unsigned rightId = predicateInfo.right.rel_id;

        auto predicateIt = predicateOrder.begin();
        auto cardinalitiesIt = estimatedCardinalities.begin();
        
        unsigned estimatedCardinality = std::max(filterSizes[leftId], filterSizes[rightId]);
        
        for (unsigned i = 0; i < estimatedCardinalities.size(); i++) {
          if (estimatedCardinality < estimatedCardinalities[i]) {
            estimatedCardinalities.insert(cardinalitiesIt, i, estimatedCardinality);
            predicateOrder.insert(predicateIt, i, predicateInfo);
            break;
          }
        }

        estimatedCardinalities.push_back(estimatedCardinality);
        predicateOrder.push_back(predicateInfo);
      }

      // Pass predicate order into join
      std::cout << joiner.join(i, predicateOrder);
    }
    *total_time = (omp_get_wtime() - start);
    display_time();

    return 0;
}
