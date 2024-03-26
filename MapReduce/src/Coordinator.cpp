#include <omnetpp.h>
#include "nlohmann/json.hpp" //libraries for JSON files
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <list>
#include <cmath>
#include <queue>
#include <deque>
#include <unordered_set>
#include "CustomClasses/TupleMessage.h"
#include "CustomClasses/IndexMessage.h"
#include "CustomClasses/IdMessage.h"
#include "CustomClasses/ReduceMessage.h"
#include "CustomClasses/PartitionMessage.h"
#include "DataStorage.h"
#include "Worker.h"

using json = nlohmann::json;
using namespace std;
using namespace omnetpp;

//declaration of all the .cpp file's functions
void read_JSON(int &, queue<string> &);
int read_CSV(vector< tuple<int,int> > &);
int convertString(string str, int start, int offset);
void print_indexPartitions(deque<vector<int>> index_v);
int randomNumber(int max, int min);
void print_memory(vector<vector<tuple<int,int>>> v);


class Coordinator: public cSimpleModule{
    private:
        bool program_finished = false; //this variable is used to know when the program is finshed

        int n_partitions; //number of partitions specified in the JSON file

        //queue of all the (map/changeKey/reduce) QueueOperations that we need to perform
        //we perform the pop of the first element only after receiving all the ack from the workers for job completed
        queue<string> QueueOperations;

        vector<tuple<int,int>> listaTuple; //this is the initial variable to store all the tuples form the CSV file

        int count_ack = 0; //counter to keep track of how many partitions are done by the workers
        int count_reduce_ack = 0; //this is the variable to keep track of how many ack reduce arrived

        vector<vector<tuple<int,int>>> memory; //this is the data structure that holds all the tuples from the csv file already divided into partitions
        int index_memory = 0; //index to iterate though memory to send each partition to the DataStorage
        cMessage* self_Memory;
        bool memorywritten = false; //this is a boolean used to know when all the partitions are written in the memory and we can start sending the index messages
        int MemorymaxDelay = 1, MemoryminDelay = 0;

        deque<vector<int>> index_v; //this is the vector containing the memory positions of each partition for each worker (the one that we need always to update)
        int index_deque = 0; //this is the index used to send each partition to one worker
        cMessage* self_Index; //self message to schedule random the time in which sending the partitions to each worker
        vector<int> workersActiveAtOperationStart;

        cMessage* self_Reduce;
        int index_reduce = 0; //to keep track of the worker to which we need to send a reduce message
        int nWorkersActiveReduce; //this is a variable used to keep track of how many workers were available when the reduce phase started

        //those are used for the hearbeats
        cMessage *timeoutMessage;
        simtime_t timeoutHeartbeat = 5;
        vector<int> workers; //at the beginning all the nodes are ready to work
        unordered_set<int> activeWorkers; //we add each time the id whenever we get an heartbeat from one worker
        int workersFailed = 0;
        vector<int> failedWorkers;

        //recovery when all nodes are failed
        cMessage *waitForRecovery;
        bool reschelingPartition = false;
        string recoveryoperation;

        vector<vector<tuple<int,int>>> failedPartitions;
        vector<int> failedIndex;
        string operationToBeRescheduled;

        cMessage* self_IndexWaitForRescheduling;
        int maxWait = 1, minWait = 0;


        int maxDelay = 2, minDelay = 1;

        //variables for statistics
        simtime_t endExecution;
        int n_messages = 0;

        simtime_t TOTresolutiontime;
        simtime_t beginresolution;


    protected:
        //list of all methods of the Coordinator Class
        virtual void initialize() override;
        virtual void handleMessage(cMessage *) override;
        int nWorkers;
        int totWorkers;
        void split_data(vector<vector<tuple<int,int>>> &v, int length);
        void split_index(deque<vector<int>> &index_v);

        void sendPartitionTuple(vector<tuple<int,int>> partition);
        void sendOperationandIndex();
        void sendreduceOperation();
        void sendHearbeats(simtime_t timeout);
        void re_scheduleOperation(vector<int> &failed);
        void deleteAllMessages();
        void collectStatistics();

    public:

};

Define_Module(Coordinator);

//method called when the simulation starts
void Coordinator::initialize(){
    cModule * parent = getParentModule();
    totWorkers = parent->par("n_workers").intValue();
    nWorkers = totWorkers;

    //at the beginning all the nodes are ready to work
    for(int i = 0; i < totWorkers; i++){
        workers.push_back(i);
    }

    //-------------    JSON READ   -----------------
    read_JSON(n_partitions, QueueOperations);

    //we set also the number of partitions in the data storage only for printing methods
    cModule *module = getParentModule()->getSubmodule("datastore");
    DataStorage *target = check_and_cast<DataStorage *>(module);
    target->setPartitions(n_partitions);


    //---------------   CSV READ    ----------------
    int length = read_CSV(listaTuple); //we need the length of the CSV to split it using also the numbers of partitions


    //----------    CREATE PARTITIONS   ------------
    split_data(memory, length);
    print_memory(memory);


    //---    SAVE PARTITIONS INTO GLOBAL MEMORY ----
    //iterate each element of the vectors of vectors of tuples and send it to the memory
    self_Memory = new cMessage("COORDINATOR SELF MEMORY");
    scheduleAt(simTime()+randomNumber(MemorymaxDelay, MemoryminDelay), self_Memory);


    //----------   CREATE VECTOR OF INDEX ----------
    //each index is the position in memory of each partition, so that every vector in the end is a vector of partitions
    split_index(index_v);
    print_indexPartitions(index_v);
    workersActiveAtOperationStart = workers;


    //--------  SEND EACH VECTOR TO ONE WORKER -----
    self_Index = new cMessage("COORDINATOR SELF INDEXES");
    scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Index);


    //-------------   SEND HEARTBEATS ------------
    sendHearbeats(timeoutHeartbeat);
}

void Coordinator::handleMessage(cMessage *msg){
    string input = (msg->getName());

    //------------------------------ SELF MESSAGES ----------------------------------
    //this situation is used very rarely, only when all the workers fail during the same cycle and there are no more active worker. The coordinator needs to wait until a new worker is alive again
    if(strcmp("COORDINATOR WAIT RECOVERY", input.c_str())==0){
        EV<< "All workers are failed, Coordinator needs to wait until a new worker is alive again"<<endl;
        if(workers.size() != 0){
            if(reschelingPartition){
                PartitionMessage* mex = new PartitionMessage(failedPartitions, failedIndex, recoveryoperation, "ADDITIONAL PARTITIONS");

                send(mex, "out", workers.at(0));

                EV<<"The number of additional partitions rescheduled is "<<failedPartitions.size()<<endl;

                failedPartitions.clear();
                failedIndex.clear();

                reschelingPartition = false;
                //after the correct rescheduling of the partitions, we can send out the heartbeats
                sendHearbeats(timeoutHeartbeat);
            }
            else{
                workersActiveAtOperationStart = workers;
                //if the size of the Queue is 1, then we need to perform the reduce phase, otherwise we perform the normal one
                if(QueueOperations.size() == 1){
                    EV<<"Performing a reduce operation with "<<workers.size()<<" total active workers"<<endl;
                    nWorkersActiveReduce = workers.size();
                    self_Reduce = new cMessage("COORDINATOR SELF REDUCE");
                    scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Reduce);
                }
                else{
                    EV<<"Performing a "<<QueueOperations.front()<<" operation with "<<workers.size()<<" total active workers"<<endl;
                    //after the first operation we need to reschedule the indexes, since it could happen that during the cycle of this operation, a failure happened
                    //and so the partitions must be rescheduled correctly since the number of active workers has changed
                    split_index(index_v);
                    print_indexPartitions(index_v);
                    index_deque = 0; //by doing so we can unlock the if in the SELF MESSAGES for self_Index and start with the next operation
                }
            }
        }
        else{
            scheduleAt(simTime()+randomNumber(maxWait, minWait), waitForRecovery);
        }

    }
    if(strcmp("COORDINATOR SELF INDEXES", input.c_str())==0){
        if(memorywritten && index_deque < index_v.size()){
            sendOperationandIndex();
        }
        scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Index);
    }
    if(strcmp("COORDINATOR SELF MEMORY", input.c_str())==0){
        if(index_memory < memory.size()){
            sendPartitionTuple(memory.at(index_memory));
            //in this case the schedule is inside this if because we know for sure that the memory is already initialized and there will not be any self_Message without a memory initialized
            scheduleAt(simTime()+randomNumber(MemorymaxDelay, MemoryminDelay), self_Memory);
        }
        else{
            memorywritten = true;
        }
    }
    if(strcmp("COORDINATOR SELF REDUCE", input.c_str())==0){
        if(index_reduce < nWorkersActiveReduce){
            sendreduceOperation();
            scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Reduce);
        }
        else{
            cancelAndDelete(msg);
        }
    }
    //this message is used rarely, only when a failure of a node happens before that each of the vector of indexes has been sent to all the workers. In order to be able to retrieve
    //the failed partitions, we need infact that each of the active workers at the beginning of each operation has its indexes
    if(strcmp("COORDINATOR SELF WAIT FOR RESCHEDULING", input.c_str())==0){
        if(QueueOperations.size() != 1){ //case of a normal operation
            if(index_deque == index_v.size()){
                re_scheduleOperation(failedWorkers);
                cancelAndDelete(msg);
            }
            else{
                scheduleAt(simTime()+randomNumber(maxWait, minWait), self_IndexWaitForRescheduling);
            }
        }
        else{ //case of a reduce operation
            if(index_reduce == nWorkersActiveReduce){
                re_scheduleOperation(failedWorkers);
                cancelAndDelete(msg);
            }
            else{
                scheduleAt(simTime()+randomNumber(maxWait, minWait), self_IndexWaitForRescheduling);
            }
        }

    }
    //time expired for heartbeats
    if(strcmp("SELF TIMEOUT", input.c_str())==0 && !program_finished){
        EV << "Hearbeat Timeout expired"<<endl;
        bool fail = false;
        //control if all the activeWorkers are the same of the workers
        if(activeWorkers.size() != workers.size()){
            beginresolution = simTime();
            fail = true;
            vector<int> failed;

            for(int i = 0; i < workers.size(); i++){
                if(activeWorkers.find(workers.at(i)) == activeWorkers.end()){ //the workers is not present between the ones that have sent the ack
                    failed.push_back(workers.at(i));
                    workersFailed++;
                }
            }

            EV<< "Nodes failed in this heartbeat cycle: "<<endl;
            for(int i: failed){
                EV << i << endl;
            }

            failedWorkers = failed;


            //here we need to assign the work of the workers failed to the ones that are ok, we distinguish between the reduce operation and all the others
            if(QueueOperations.size() == 1){
                //before re scheduling, I need to be sure that every reduce module has been sent to all the active workers
                if(index_reduce == nWorkersActiveReduce){
                    re_scheduleOperation(failedWorkers);
                }
                else{
                    self_IndexWaitForRescheduling = new cMessage("COORDINATOR SELF WAIT FOR RESCHEDULING");
                    scheduleAt(simTime()+randomNumber(maxWait, minWait), self_IndexWaitForRescheduling);
                }
            }
            else{
                //before re scheduling, I need to be sure that every index has been sent to all the active workers, so i need to use a self message
                if(index_deque == index_v.size()){ //if all indexes have been scheduled
                    re_scheduleOperation(failedWorkers);
                }
                else{ //otherwise we need to wait
                    self_IndexWaitForRescheduling = new cMessage("COORDINATOR SELF WAIT FOR RESCHEDULING");
                    scheduleAt(simTime()+randomNumber(maxWait, minWait), self_IndexWaitForRescheduling);
                }
            }

        }
        //at the end we set the workers = activeworkers
        workers.clear();
        EV << "Active Workers: "<<endl;
        for (auto itr : activeWorkers){
            workers.push_back(itr);
            EV<< itr <<endl;
        }
        activeWorkers.clear();
        nWorkers = workers.size();

        //to send the hearbeats we need to know if a fail was detected or not
        //if the fail was not detected we can send them without any problem
        if(!fail){
            sendHearbeats(timeoutHeartbeat);
        }
        //otherwise we need to wait for a correct reschedule of all the partitions, and then we can send out the hearbeats
    }

    //--------------------- HEARTBEAT ------------------------
    //each time an heartbeat arrives, we save the worker from which this message has arrived
    if(strcmp("HEARTBEAT", input.c_str())==0){
        cGate *inGate = msg->getArrivalGate();
        int inGateIndex = inGate->getIndex();
        EV << "Received heartbeat ACK message from worker " << inGateIndex<<endl;
        activeWorkers.insert(inGateIndex);
    }

    //----------------- FAILED PARTITIONS ------------------------
    //it is the PartitionMessage from the failed node, with the partitons and the memoryindexes to be sent to another active node
    if(strcmp("FAILED PARTITIONS", input.c_str())==0){
        PartitionMessage* mess = dynamic_cast<PartitionMessage*>(msg);

        if(mess->getVector().size() != 0){
            operationToBeRescheduled = mess->getOperation();
        }

        //since the worker could also have finished it's work, we control if the boolean variable finished is equal to false
        if(!mess->getFinish()){
            //we save locally the partitions failed and the indexes failed
            vector<vector<tuple<int,int>>> failed = mess->getVector();
            vector<int> index = mess->getIndeces();

            for(vector<tuple<int,int>> part: failed){
                failedPartitions.push_back(part);
            }
            for(int i: index){
                failedIndex.push_back(i);
            }
        }
        //otherwise the message is dropped and it is not added to the list of partitions that needs to be rescheduled

        workersFailed--;
        //we do this, because if two or more workers fails in the same cycle, then we need to send heartbeats only one time, so after all the failed partition
        //messages have been arrived
        if(workersFailed == 0){
            TOTresolutiontime += simTime() - beginresolution;
            //once we have received all the partitions and indexes failed we can send them to a node active
            recoveryoperation = operationToBeRescheduled;
            PartitionMessage* mex = new PartitionMessage(failedPartitions, failedIndex, operationToBeRescheduled, "ADDITIONAL PARTITIONS");
            if(failedPartitions.size() != 0){
                n_messages++; //for statistics

                if(workers.size() == 0){
                    waitForRecovery = new cMessage("COORDINATOR WAIT RECOVERY");
                    reschelingPartition = true;
                    scheduleAt(simTime()+randomNumber(maxWait, minWait), waitForRecovery);
                }
                else{
                    send(mex, "out", workers.at(0));

                    EV<<"The number of additional partitions rescheduled is "<<failedPartitions.size()<<endl;

                    failedPartitions.clear();
                    failedIndex.clear();

                    //after the correct rescheduling of the partitions, we can send out the heartbeats
                    sendHearbeats(timeoutHeartbeat);
                }
            }
            else{
                EV<<"The number of additional partitions rescheduled is "<<failedPartitions.size()<<endl;

                failedPartitions.clear();
                failedIndex.clear();

                //after the correct rescheduling of the partitions, we can send out the heartbeats
                sendHearbeats(timeoutHeartbeat);
            }

        }
    }

    //------------- RESTARTING OF A FAILED NODE ---------------------
    if(strcmp("ALIVE", input.c_str())==0){
        cGate *inGate = msg->getArrivalGate();
        int inGateIndex = inGate->getIndex();
        EV << "The worker " << inGateIndex<<" is alive again"<<endl;
        activeWorkers.insert(inGateIndex);
        workers.push_back(inGateIndex);
    }

    //------------------- ACK OPERATIONS ---------------------
    //every time a worker finishes its operation, it sends an ack, and when the number of ack = partitions, then we can pass to the next action
    if(strcmp("ACK", input.c_str())==0){;
        count_ack++;
        EV<< "Total number of ack  "<<count_ack<<endl;
        if(count_ack == n_partitions){
            bubble("Operation finished");
            count_ack = 0;
            QueueOperations.pop(); //removing the operation already done
            workersActiveAtOperationStart = workers;
            EV<< "Operators still to be computed: "<<QueueOperations.size()<<endl;


            if(workers.size() == 0){
                waitForRecovery = new cMessage("COORDINATOR WAIT RECOVERY");
                scheduleAt(simTime()+randomNumber(maxWait, minWait), waitForRecovery);
            }
            else{
                //if the size of the Queue is 1, then we need to perform the reduce phase, otherwise we perform the normal one
                if(QueueOperations.size() == 1){
                    EV<<"Performing a reduce operation with "<<workers.size()<<" total active workers"<<endl;
                    nWorkersActiveReduce = workers.size();
                    self_Reduce = new cMessage("COORDINATOR SELF REDUCE");
                    scheduleAt(simTime()+randomNumber(maxDelay, minDelay), self_Reduce);
                }
                else{
                    EV<<"Performing a "<<QueueOperations.front()<<" operation with "<<workers.size()<<" total active workers"<<endl;
                    //after the first operation we need to reschedule the indexes, since it could happen that during the cycle of this operation, a failure happened
                    //and so the partitions must be rescheduled correctly since the number of active workers has changed
                    split_index(index_v);
                    print_indexPartitions(index_v);
                    index_deque = 0; //by doing so we can unlock the if in the SELF MESSAGES for self_Index and start with the next operation
                }
            }

        }
    }
    //---------------------- ACK REDUCE ----------------------
    //when each worker finishes its reduce operation, it sends an ack, so when the acks are equal to the numbers of workers (active when the reduce phase started)
    if(strcmp("REDUCE-ACK", input.c_str())==0){
        count_reduce_ack++;
        if(count_reduce_ack == nWorkersActiveReduce){
            endExecution = simTime();
            //the program is finished, so we print out the final content of the DataStore
            cModule *module = getParentModule()->getSubmodule("datastore");
            DataStorage *target = check_and_cast<DataStorage *>(module);
            target->printFinalMemory();

            program_finished = true;
            collectStatistics();
            deleteAllMessages();
        }
    }
}

void Coordinator::collectStatistics(){
    recordScalar("END", endExecution);
    recordScalar("TOTRESOLUTIONTIME", TOTresolutiontime);

    DataStorage *datastorage = check_and_cast<DataStorage *>(getParentModule()->getSubmodule("datastore"));

    int totMessages = n_messages;
    int totFailures = 0;
    simtime_t TOTfailuretime;

    totMessages += datastorage->n_messages;
    for(int i = 0; i < totWorkers; i++){
        Worker *worker = check_and_cast<Worker *>(getParentModule()->getSubmodule("workers", i));
        totFailures += worker->counterFailures;
        totMessages += worker->n_messages;
        TOTfailuretime += worker->TOTfailuretime;
    }


    recordScalar("TOT-FAILURES",totFailures);
    recordScalar("TOT-FAILURETIME", TOTfailuretime);
    recordScalar("TOT-MESSAGES",totMessages);
    recordScalar("% MEAN-FAILURETIME", (TOTfailuretime/totWorkers) / endExecution);
    recordScalar("MEAN-RECOVERYTIME", TOTfailuretime/totFailures);
}

//method to schedule again the jobs of a failed worker
void Coordinator::re_scheduleOperation(vector<int> &failed){
    //we need to handle all the nodes that have failed (in general is only one but they could be also more than one)
    for(int i: failed){
        //we retrieve the input partitions with a message to the worker
        cMessage* msg = new cMessage("FAILURE OPERATION");
        n_messages++; //for statistics
        send(msg, "out", i);
        //we get back the FAILED PARTITION message from the failed node
    }

    failed.clear();
}

//method to send heartbeats to all the active nodes of the network
void Coordinator::sendHearbeats(simtime_t timeout){
    if(!program_finished){
        timeoutMessage = new cMessage("SELF TIMEOUT");
        EV<<"Sending heartbeats to "<<workers.size()<<" workers"<<endl;
        for (int i=0; i < workers.size(); i++){
           cMessage * msg = new cMessage("HEARTBEAT");
           n_messages++; //for statistics
           send(msg,"out", workers.at(i));
        }
        scheduleAt(simTime() + timeout, timeoutMessage);
    }
}

//method to send to the correspondant worker, the value of the modulo which they are encharge of
void Coordinator::sendreduceOperation(){
    ReduceMessage *msg = new ReduceMessage(index_reduce, nWorkersActiveReduce, "REDUCE");
    n_messages++; //for statistics
    send(msg, "out", workersActiveAtOperationStart.at(index_reduce));
    index_reduce++;
}

//method to send one partition of tuples to the DataStorage
void Coordinator::sendPartitionTuple(vector<tuple<int,int>> partition){
    TupleMessage *msg = new TupleMessage(partition, "TUPLES");
    n_messages++; //for statistics
    send(msg, "out_mem");
    index_memory++;
}

//method to send one Partition of indexes to one worker
void Coordinator::sendOperationandIndex(){
    string nextoperation = QueueOperations.front();
    IndexMessage *msg = new IndexMessage(index_v.at(index_deque), nextoperation, "INDEX");
    n_messages++; //for statistics
    send(msg, "out", workersActiveAtOperationStart.at(index_deque));
    index_deque++;
}

//method to split the list of all tuples of length = length in the n_partiotions
void Coordinator::split_data(vector<vector<tuple<int,int>>> &v, int length){
    //we create a number of vector equals to the n_partitions
    for(int i=0; i<n_partitions; i++){
         vector<tuple<int,int>> k;
         v.push_back(k);
    }
    EV<<endl<<"The number of partitions chosen for this simulation is: "<<v.size()<<endl<<endl;

    //we iterate each tuple in the csv file and we add each time one of them into one of the #n_partitons vectors
    int j=0;
    for(int i=0; i<length; i++){
        v.at(j).push_back(listaTuple.at(i));
        j++;
        if(j==n_partitions){
            j=0;
        }
    }
}

//method to assign the different index for each worker of the network
void Coordinator::split_index(deque<vector<int>> &index_v){
    index_v.clear();

    //this is only for creating the vectors inside the deque
    for(int i=0; i<nWorkers; i++){
         vector<int> k;
         index_v.push_back(k);
    }
    //EV<<endl<<index_v.size()<<endl<<endl;

    //here we add each index inside each partition in the deque
    int j=0;
    for(int i=0; i<n_partitions; i++){
        index_v.at(j).push_back(i);
        j++;
        if(j==nWorkers){j=0;}
    }


}

//method to read the JSON file
void read_JSON(int &n_partitions, queue<string> &QueueOperations){
    ifstream f("InputCoordinator/test.json", ifstream::in);

    json fileJson; //empty Json object
    f >> fileJson; //we put what we read (f) in the empty Json object

    n_partitions = fileJson.at("partitions");

    //we save in the queue all the QueueOperations to be done
    for(string s : fileJson.at("instructions")){
        QueueOperations.push(s);
    }
}

//method to get information from the CSV file
int read_CSV(vector< tuple<int,int> > &listaTuple){
    ifstream fileCSV;
    string str;
    fileCSV.open("InputCoordinator/test.csv", ios::in);
    getline(fileCSV,str); //skip the first line of the CSV file (we have the two titles of the two columns)

    int count = 0;
    EV<<"These are all the tuples written in the .csv file: "<<endl;
    //looping through all the lines of the CSV file
    while(getline(fileCSV,str)){
        tuple<int,int> t;
        size_t pos= str.find(";");

        get<0>(t) = convertString(str, 0, pos);
        get<1>(t) = convertString(str, pos+1, str.length()-pos-1);

        EV << get<0>(t) << " " << get<1>(t) << endl;
        listaTuple.push_back(t);
        count++;
    }

    fileCSV.close();
    return count;
}

//method used in read_CSV to convert a string with an integer into a integer type
int convertString(string str, int start, int offset){
    int n;
    stringstream ss;
    string token = str.substr(start, offset);
    ss << token;
    ss >> n;
    return n;
}

//method used to print every time the new scheduling of the partitions
void print_indexPartitions(deque<vector<int>> index_v){
    EV<< "Scheduling of the indexes based on the active workers"<<endl;
    for(int i=0; i<index_v.size();i++){
        EV<<"Group "<<i<<":"<<endl;
        for(int j=0; j<index_v.at(i).size();j++){
            EV<<index_v.at(i).at(j)<<endl;
        }
        EV<<endl;
    }
}

//simple method to print the splitted partitions in the coordinator after reading the input
void print_memory(vector<vector<tuple<int,int>>> v){
    for(int i=0; i<v.size(); i++){
        EV<<"Partition "<<i<<":"<<endl;
        for(int k=0; k<v.at(i).size(); k++){
            EV<<get<0>(v.at(i).at(k))<<","<<get<1>(v.at(i).at(k))<<endl;
        }
        EV<<endl;
    }
}

int randomNumber(int max, int min){
    return rand()%(max-min+1) + min;
}

void Coordinator::deleteAllMessages(){
    cancelAndDelete(self_Index);

    cModule *module = getParentModule()->getSubmodule("datastore");
    DataStorage *target = check_and_cast<DataStorage *>(module);
    target->cancelMessages();

    for(int i = 0; i < totWorkers; i++){
        Worker *worker = check_and_cast<Worker *>(getParentModule()->getSubmodule("workers", i));
        worker->cancelMessages();
    }
}

