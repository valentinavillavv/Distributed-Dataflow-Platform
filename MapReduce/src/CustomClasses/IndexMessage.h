/*
 * IndexMessage.h
 *
 *  Created on: 4 apr 2023
 *      Author: valen
 */

#ifndef CUSTOMCLASSES_INDEXMESSAGE_H_
#define CUSTOMCLASSES_INDEXMESSAGE_H_

class IndexMessage: public omnetpp::cMessage{
    protected:
        std::vector<int> index;
        std::string operation;
    public:
        IndexMessage(std::vector<int> vec, std::string op, const char* name=NULL, short kind=0):cMessage(name,kind){
           index = vec;
           operation = op;
        }

        virtual ~IndexMessage() {}

        void setIndex(std::vector<int> o){
            index=o;
        }

        std::vector<int> getIndex(){
            return index;
        }

        void setOperation(std::string o){
            operation=o;
        }

        std::string getOperation(){
            return operation;
        }
};


#endif /* CUSTOMCLASSES_INDEXMESSAGE_H_ */
