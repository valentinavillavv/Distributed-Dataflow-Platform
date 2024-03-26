#ifndef DATASTORAGE_H_
#define DATASTORAGE_H_

#include <vector>
#include "CustomClasses/PartitionMessage.h"
#include "CustomClasses/TupleMessage.h"
#include <deque>

using namespace omnetpp;
using namespace std;

class DataStorage: public cSimpleModule{
    private:
        vector<vector<tuple<int,int>>> memory;
        vector<tuple<int,int>> finalmemory;
        //this counter is used only to count when the message of single partitions is equal to 9, to print out the new values in memory
        int counter = 0;

        cMessage* self_Index;
        deque<PartitionMessage*> queuePartitionMessage; //this is the queue for sending out the partitions to each worker that asked for them
        deque<int> queueGates; //this is a queue to know the gate to which send the PartitionMessage saved in the previous queue

        cMessage* self_Reduce;
        deque<TupleMessage*> queueTupleMessage; //this is the queue for sending out the tuples that each worker needs for the reduce phase

        bool finished = false;

        int maxDelay = 4, minDelay = 1;

        int n_partitions;

    protected:
        virtual void initialize() override;
        virtual void handleMessage(cMessage *msg) override;
        void printMemory(int &counter);
        int randomNumber(int max, int min);


    public:
        void printFinalMemory();
        void cancelMessages();
        void setPartitions(int p);
        //statistic collection
        int n_messages = 0; //for statistics
};

#endif /* DATASTORAGE_H_ */
