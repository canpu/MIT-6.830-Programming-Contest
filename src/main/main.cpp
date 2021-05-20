#include <iostream>

#include "joiner.h"
#include "parser.h"
#include <vector>
#include "omp.h"


int main(int argc, char *argv[]) {
    Joiner joiner;

    // Read join relations
    std::string line;
    while (getline(std::cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    QueryInfo i;
    while (getline(std::cin, line)) {
        if (line == "F") continue; // End of a batch
        i.parseQuery(line);
        std::cout << joiner.join(i);
    }

    return 0;
}
