/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_FILE_HH
#define ORC_FILE_HH

#include <string>
#include "Reader.hh"

/** @file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace orc {

static const std::string MAGIC("ORC");

    class Version {
    private:
     std::string name;
     int major;
     int minor;

    public:
     Version(std::string name, int major, int minor) {
         this->name = name;
         this->major = major;
         this->minor = minor;
     }

     std::string getName()   { return this->name; }
     int getMajor()   { return this->major; }
     int getMinor()   { return this->minor; }
    };

    static const Version V_0_11("0.11", 0, 11);
    static const Version V_0_12("0.12", 0, 12);
    //static std::unordered_map<std::string, Version> Versions({{"0.11", V_0_11},{"0.12", V_0_12}});
    static const Version CURRENT_VERSION = V_0_12;
}
#endif
