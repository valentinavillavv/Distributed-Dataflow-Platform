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

// TupleMessage.h
#ifndef __MYMESSAGE_H_
#define __MYMESSAGE_H_

#include <vector>
#include <tuple>
#include "omnetpp.h"

class TupleMessage : public omnetpp::cMessage {
protected:
    std::vector<std::tuple<int, int>> myVector;

    //this variable is used to know if when a worker fails, it has finished it's job or not
    bool finish;

public:
    TupleMessage(std::vector<std::tuple<int, int>> v, const char *name=NULL, short kind=0) : cMessage(name, kind) {
        myVector = v;
        finish = false;
    }
    virtual ~TupleMessage() {}

    // Add a tuple to the vector
    /*void addTuple(int x, int y) {
        myVector.push_back(std::make_tuple(x, y));
    } */

    //to save the vector passed as input in the variable of the TupleMessage class
    void setVector(std::vector<std::tuple<int, int>> v){
        myVector = v;
    }

    // Get the vector
    std::vector<std::tuple<int, int>> getVector() const {
        return myVector;
    }

    void changeFinish(){
        finish = !finish;
    }

    bool getFinish(){
        return finish;
    }
};

#endif /* __TupleMessage_H_ */

