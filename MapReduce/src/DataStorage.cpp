#include <omnetpp.h>
#include <vector>
#include <tuple>
#include "CustomClasses/TupleMessage.h"
#include "CustomClasses/IndexMessage.h"
#include "CustomClasses/PartitionMessage.h"
#include "CustomClasses/SinglePartitionMessage.h"
#include "CustomClasses/ReduceMessage.h"
#include "DataStorage.h"
#include <fstream>

Define_Module(DataStorage);

void DataStorage::initialize(){
    self_Index = new cMessage("DATASTORAGE SELF INDEX");
    scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Index);
    self_Reduce = new cMessage("DATASTORAGE SELF REDUCE");
    scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Reduce);
}

void DataStorage::handleMessage(cMessage *msg){
    string input = (msg->getName());

    //-------------------- SELF MESSAGES ----------------------
    if(msg == self_Index && !finished){
        if(!queuePartitionMessage.empty()){
            send(queuePartitionMessage.front(),"out",queueGates.front());
            n_messages++; //for statistics
            queuePartitionMessage.pop_front();
            queueGates.pop_front();
        }
        scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Index);
    }
    if(msg == self_Reduce && !finished){
        if(!queueTupleMessage.empty()){
            send(queueTupleMessage.front(),"out",queueGates.front());
            n_messages++; //for statistics
            queueTupleMessage.pop_front();
            queueGates.pop_front();
        }
        scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Reduce);
    }

    //----------------    MESSAGE WITH TUPLES   ---------------
    //this message is received from the coordinator to save the tuples at the beginning of the simulation
    if(strcmp("TUPLES", input.c_str()) == 0){
        TupleMessage* mess = dynamic_cast<TupleMessage*>(msg);
        memory.push_back(mess->getVector());

        printMemory(counter);
    }

    //----------------    MESSAGE WITH INDECES   --------------
    //this is the message from a worker to retrieve some partitions of data
    if(strcmp("INDEX", input.c_str()) == 0){
        IndexMessage* mess = dynamic_cast<IndexMessage*>(msg);

        //we cycle the vector of indeces and we return the vector of tuples for each index
        vector<int> indeces = mess->getIndex();
        vector<vector<tuple<int,int>>> partitionVector;
        for(int i = 0; i < indeces.size(); i++){
            int index = indeces.at(i);
            partitionVector.push_back(memory.at(index));
        }

        //we create the message to be send to the worker, but we save the gate and the message into two deques
        PartitionMessage *tuplemess = new PartitionMessage(partitionVector, indeces, mess->getOperation(), "PARTITIONS");
        queuePartitionMessage.push_back(tuplemess);
        cGate *inGate = msg->getArrivalGate();
        int inGateIndex = inGate->getIndex();
        queueGates.push_back(inGateIndex);

    }

    //--------- MESSAGE WITH ONE PARTITION TO WRITE IN MEMORY -----
    //this is the message send by the worker to overwrite a partition with a certain index
    if(strcmp("WRITEPARTITION", input.c_str()) == 0){
        SinglePartitionMessage* mess = dynamic_cast<SinglePartitionMessage*>(msg);
        memory.at(mess->getIndex()) = mess->getVector();
        printMemory(counter);
    }

    //---------------------- REDUCE MESSAGE -----------------------
    //this is the final message, we need to find out the tuples with key%n_workers = modulo
    if(strcmp("REDUCE", input.c_str()) == 0){
        ReduceMessage* mess = dynamic_cast<ReduceMessage*>(msg);
        int nWorkers = mess->getWorkers();
        int module = mess->getModule();

        vector<tuple<int,int>> partition;

        for(int i = 0; i < memory.size(); i++){
            for(int j = 0; j < memory.at(i).size(); j++){
                int key = get<0>(memory.at(i).at(j));

                if(key%nWorkers == module){
                    partition.push_back(memory.at(i).at(j));
                }
            }
        }

        TupleMessage *messs = new TupleMessage(partition, "REDUCETUPLES");
        queueTupleMessage.push_back(messs);
        cGate *inGate = msg->getArrivalGate();
        int inGateIndex = inGate->getIndex();
        queueGates.push_back(inGateIndex);

    }

    //-------------------- FINAL REDUCE MESSAGE -------------------
    //this is the message from the workers with the vector of tuples of the reduce result
    if(strcmp("REDUCERESULTS", input.c_str()) == 0){
        TupleMessage* mess = dynamic_cast<TupleMessage*>(msg);
        vector<tuple<int,int>> v = mess->getVector();

        //we append the vector of results to the finalmemory
        for(tuple<int,int> t: v){
            finalmemory.push_back(t);
        }
        //finalmemory.insert(finalmemory.end(), v.begin(), v.end());
    }

}

void DataStorage::printMemory(int &counter){
    counter++;

    if(counter == n_partitions){
        EV<<"The operator is correctly performed, the new memory saved is:"<<endl;
        for(int i = 0; i < memory.size(); i++){
            for(int j = 0; j < memory.at(i).size(); j++){
                EV << get<0>(memory.at(i).at(j))<< ", ";
                EV << get<1>(memory.at(i).at(j))<< endl;
            }
        }
        EV<< "------------------"<<endl;
        counter = 0;
    }


}

void DataStorage::printFinalMemory(){
    ofstream outputFile("output.txt");

    if (!outputFile.is_open()) {
        // Handle file opening error
        return;
    }
    Enter_Method("printFinalMemory"); //this line of code is necessary to make a change of context passing from the execution of Coordinator to DataStorage
    EV<<"The simulation has finished correctly. The final memory:"<<endl;
    for(int i = 0; i < finalmemory.size(); i++){
        EV << get<0>(finalmemory.at(i))<< ", ";
        EV << get<1>(finalmemory.at(i))<< endl;

        outputFile << get<0>(finalmemory.at(i)) << "," <<get<1>(finalmemory.at(i)) << "\n";
    }

    outputFile.close();
}

void DataStorage::cancelMessages(){
    finished = true;
}

int DataStorage::randomNumber(int max, int min){
    return rand()%(max-min+1) + min;
}

void DataStorage::setPartitions(int p){
    n_partitions = p;
}
