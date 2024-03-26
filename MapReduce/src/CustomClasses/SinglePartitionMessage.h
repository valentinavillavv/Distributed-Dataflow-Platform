//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
// 

#ifndef CUSTOMCLASSES_SINGLEPARTITIONMESSAGE_H_
#define CUSTOMCLASSES_SINGLEPARTITIONMESSAGE_H_

class SinglePartitionMessage: public omnetpp::cMessage{
protected:
    //single new partition to be written in the memory
    std::vector<std::tuple<int, int>> myVector;
    //index of the memory associated to the new partition
    int index;

public:
    SinglePartitionMessage(std::vector<std::tuple<int, int>> v, int i, const char *name=NULL, short kind=0) : cMessage(name, kind) {
        myVector = v;
        index = i;
    }
    virtual ~SinglePartitionMessage() {}

    void setVector(std::vector<std::tuple<int, int>> v){
        myVector = v;
    }

    // Get the vector
    std::vector<std::tuple<int, int>> getVector() const {
        return myVector;
    }

    int getIndex() const {
        return index;
    }
};

#endif /* CUSTOMCLASSES_SINGLEPARTITIONMESSAGE_H_ */
