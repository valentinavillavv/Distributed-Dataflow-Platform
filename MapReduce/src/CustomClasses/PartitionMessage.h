/*
 * PartitionMessage.h
 *
 *  Created on: 10 apr 2023
 *      Author: Torros
 */

#ifndef CUSTOMCLASSES_PARTITIONMESSAGE_H_
#define CUSTOMCLASSES_PARTITIONMESSAGE_H_

class PartitionMessage: public omnetpp::cMessage {
    protected:
        //this is the vector containing all the partitions from the memory
        std::vector<std::vector<std::tuple<int, int>>> myVector;
        //this is the vector of all the indices of each partition
        std::vector<int> indeces;

        std::string operation; //this is the operation that we need to perform example "map(ADD(3))"

        //this variable is used to know if when a worker fails, it has finished it's job or not
        bool finish;

    public:
        PartitionMessage(std::vector<std::vector<std::tuple<int, int>>> v, std::vector<int> i, std::string o, const char *name=NULL, short kind=0) : cMessage(name, kind) {
            myVector = v;
            indeces = i;
            operation = o;
            finish = false;
        }
        virtual ~PartitionMessage() {}


        void setVector(std::vector<std::vector<std::tuple<int, int>>> v){
            myVector = v;
        }

        // Get the vector
        std::vector<std::vector<std::tuple<int, int>>> getVector() const {
            return myVector;
        }

        std::vector<int> getIndeces() const {
            return indeces;
        }

        std::string getOperation(){
            return operation;
        }

        void changeFinish(){
            finish = !finish;
        }

        bool getFinish(){
            return finish;
        }
};

#endif /* CUSTOMCLASSES_PARTITIONMESSAGE_H_ */
