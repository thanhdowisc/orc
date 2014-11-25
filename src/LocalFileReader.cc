#include <stdlib.h>
#include <string>
#include <vector>
#include <algorithm>
#include <memory>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <iomanip>
#include <boost/any.hpp>

#include "orc/Reader.hh"
#include "orc/OrcFile.hh"
#include "orc_proto.pb.h"


using namespace orc;

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Read the input arguments
    const char ROWINDEX_PREFIX[] = "--rowindex=";
    const int ROWINDEX_PREFIX_LENGTH = strlen(ROWINDEX_PREFIX);

    std::vector<char*> files;
    std::vector<int> rowIndexCols;

    char* pChar ;
    for (int i=1; i<argc; i++)
    {
        if (argv[i][0]=='-' & argv[i][1]=='-') {
            pChar = strstr(argv[i], ROWINDEX_PREFIX);
            if (pChar == argv[i]) { // the argument string starts with ROWINDEX_PREFIX
                pChar = strtok(argv[i]+ROWINDEX_PREFIX_LENGTH,",");
                while (pChar != NULL)
                {
                    rowIndexCols.push_back(atoi(pChar));
                    pChar = strtok(NULL,",");
                };
            } else {
                std::cout << "Unknown argument " << argv[i] << std::endl ;
                std::cout << "Usage: localfiledump <filename1> [... <filenameN>] [--rowindex=i1,i2,...,iN]" << std::endl ;
            };
        } else {
            files.push_back(argv[i]);
        };
    };

    for(int fileIx=0; fileIx<files.size(); fileIx++) {
        std::cout << "Reading file" << files[fileIx] << std::endl;
        orc::Reader* reader = orc::createReader(orc::readLocalFile(std::string(files[fileIx])));

        int rowCounter = 0;

        long value;
        std::string str;

        while (reader->hasNext()) {
            std::vector<boost::any> row = reader->next();

            for (int columnIx=0; columnIx < row.size(); columnIx++) {
                // This is a temporary hack since we don't yet have a method to get column data types
                // We only handle integers and strings
                try {
                     value = boost::any_cast<long>(row[columnIx]);
                     std::cout<< value << " | " ;
                } catch(const boost::bad_any_cast &) {
                    // This column is not an integer
                }

                try {
                    str = boost::any_cast<ORC_STRING>(row[columnIx]);
                    std::cout<< str << " | " ;
                } catch(const boost::bad_any_cast &) {
                    // This column is not a string
                }
            }
            std::cout << std::endl ;
        };

        delete reader;
    };

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}



