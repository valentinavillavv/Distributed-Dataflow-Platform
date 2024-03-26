#include <omnetpp.h>
#include "CustomClasses/TupleMessage.h"
#include "CustomClasses/IndexMessage.h"
#include "CustomClasses/PartitionMessage.h"
#include "CustomClasses/SinglePartitionMessage.h"
#include "CustomClasses/ReduceMessage.h"
#include "CustomClasses/IdMessage.h"
#include <vector>
#include <tuple>
#include <deque>
#include <cstdlib>
#include <string.h>
#include <algorithm>
#include "Worker.h"




Define_Module(Worker);

void Worker::initialize(){
    ID = getIndex();

    self_Operation = new cMessage("WORKER SELF OPERATION");
    scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_Operation);
}

void Worker:: handleMessage(cMessage *msg){
    //since we will have more types of messages incoming to a worker, we need to distinguish based on the name of the message
    string input = (msg->getName());

    //----------------------    SELF MESSAGES --------------------
    if(strcmp("WORKER SELF OPERATION", input.c_str())==0 && !failed && !finished){
        if(!input_data.empty()){
            vector<tuple<int,int>> output_partition = performOperation(input_data.at(0), memoryIndeces.at(0));

            //send the output_data to the memory
            SinglePartitionMessage *partmess = new SinglePartitionMessage(output_partition, memoryIndeces.at(0), "WRITEPARTITION");
            n_messages++; //for statistics
            send(partmess,"out_mem");

            input_data.erase(input_data.begin()); //we can remove the partition from the input data since it is saved in the global memory
            memoryIndeces.erase(memoryIndeces.begin());

            //we send an ACK for each partition
            cMessage* msg = new cMessage("ACK");
            n_messages++; //for statistics
            send(msg,"out");
        }
        scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_Operation);
    }
    else if(strcmp("WORKER SELF REDUCE RESULT", input.c_str())==0 && !failed && !finished){
        if(!input_data.empty()){
            vector<tuple<int,int>> output_partition = perfromReduceOperation(input_data.at(0));

            TupleMessage *tmess = new TupleMessage(output_partition, "REDUCERESULTS");
            n_messages++; //for statistics
            send(tmess, "out_mem");

            input_data.erase(input_data.begin());

            //we send an ACK to the coordinator
            cMessage* ackmsg = new cMessage("REDUCE-ACK");
            n_messages++; //for statistics
            send(ackmsg,"out");
        }
        scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_OperationReduce);
    }
    else if(strcmp("SELF HEARTBEAT", input.c_str())==0){
        //probability of loosing the packet
        double p = uniform(0,1);
        EV<< p;
        if(p < pOfFailure){
            bubble("Message lost");
            counterFailures++;
            failed = true;
            beginfailure = simTime();
        }
        else{
            cMessage *heartbeat = new cMessage("HEARTBEAT");
            n_messages++; //for statistics
            send(heartbeat, "out");
        }
        //heartbeat = nullptr;
    }
    else if(strcmp("WORKER SELF RESTART", input.c_str())==0){
        //cancelAndDelete(msg);
        double p = uniform(0,1);
        EV<<"Restarting "<<p<<endl;

        if(p < pRestarting){
            bubble("Alive Again");
            TOTfailuretime += simTime() - beginfailure;
            failed = false;
            waitingForPartition = false;
            detectedFailureFromCoordinator = false;

            cMessage * mess = new cMessage("ALIVE"); //we have to update the coordinator that this node is live again
            n_messages++; //for statistics
            send(mess,"out");

            if(!finished){
                if(operand == "reduce"){
                    if(self_OperationReduce != nullptr){
                        cancelEvent(self_OperationReduce);
                    }
                    self_OperationReduce = new cMessage("WORKER SELF REDUCE RESULT");
                    scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_OperationReduce);
                }
                else{
                    if(self_Operation != nullptr){
                        cancelEvent(self_Operation);
                    }
                    self_Operation = new cMessage("WORKER SELF OPERATION");
                    scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_Operation);
                }
            }
        }
        else{
            self_Restart = new cMessage("WORKER SELF RESTART");
            scheduleAt(simTime()+randomNum(maxRestart, minRestart), self_Restart);
        }
    }

    //------------------    HEARBEAT    -----------------
    else if(strcmp("HEARTBEAT", input.c_str())==0){
        self_heartbeat = new cMessage("SELF HEARTBEAT");
        scheduleAt(simTime()+randomNum(maxHeartDelay, minHeartDelay), self_heartbeat);
    }

    //----------   MESSAGE WITH OPERAND AND INDICES  --------
    //this is the message received from the coordinator that starts the process of retrieving data from the memory
    else if(strcmp("INDEX", input.c_str())==0){
        waitingForPartition = true;
        IndexMessage* mess = dynamic_cast<IndexMessage*>(msg);

        operationToDo = mess->getOperation();
        typeOfOperation(mess-> getOperation());

        //we send a message to the memory to retrieve the tuples
        n_messages++; //for statistics
        send(mess, "out_mem");
    }

    //----------------   MESSAGE WITH MODULE  ---------------
    //this is the last message received from the coordinator, to start the reduce phase
    else if(strcmp("REDUCE", input.c_str())==0){
        ReduceMessage* mess = dynamic_cast<ReduceMessage*>(msg);
        waitingForPartition = true;
        operationToDo = "reduce()";
        operand = "reduce";

        if(self_Operation != nullptr){
            cancelEvent(self_Operation); //we need to delete this message, because from now on, only the reduce phase must be done
        }

        n_messages++; //for statistics
        send(mess, "out_mem");
    }

    //---------------    MESSAGE WITH TUPLES   --------------
    //this is the message from the memory with the tuples
    else if(strcmp("PARTITIONS", input.c_str()) == 0){
        PartitionMessage* mess = dynamic_cast<PartitionMessage*>(msg);
        for(vector<tuple<int,int>> p: mess->getVector()){
            input_data.push_back(p);
        }
        for(int i: mess->getIndeces()){
            memoryIndeces.push_back(i);
        }

        waitingForPartition = false;

        if(detectedFailureFromCoordinator){ //this is the case in which the node has failed from the point of view of the Coordinator, and we are waiting the partitions from the memory
            sendFailureOperation();
        }
     }

    //----------------    REDUCE TUPLES    --------------
    else if(strcmp("REDUCETUPLES", input.c_str())==0){
        TupleMessage* mess = dynamic_cast<TupleMessage*>(msg);
        input_data.push_back(mess->getVector());
        waitingForPartition = false;

        if(detectedFailureFromCoordinator){
            sendFailureOperation();
        }
        else{
            if(self_OperationReduce == nullptr){
                self_OperationReduce = new cMessage("WORKER SELF REDUCE RESULT");
                scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_OperationReduce);
            }
        }
    }

    //--------------    FAILURE OF THIS NODE    --------------
    //this message is sent by the coordinator to get the tuples and indices of this failed worker
    else if(strcmp("FAILURE OPERATION", input.c_str())==0){
        detectedFailureFromCoordinator = true;
        if(!waitingForPartition){ //if the worker is not waiting for the partitions from the memory, but it has already them
            sendFailureOperation();
        }
        //in case the worker is waiting the partitions, then it will call the sendFailureOperation() when the partition from the memory will arrive
    }

    //--------------    ADDITIONAL WORK    --------------
    //this message is sent by the coordinator to do some extra jobs of some failed workers
    else if(strcmp("ADDITIONAL PARTITIONS", input.c_str())==0){
        PartitionMessage* mess = dynamic_cast<PartitionMessage*>(msg);

        //we need to update the operation that we need to do, because it can happen that the worker wake up during a phase, and receives the tuples as additional work, even if in this phase it hasn't done something yet
        operationToDo = mess->getOperation();
        if(operationToDo == "reduce()"){
            //EV<< "additional partitions "<<mess->getOperation()<<endl;
            operand = "reduce";
        }
        else{
            //EV<< "additional partitions "<<mess->getOperation()<<endl;
            typeOfOperation(mess->getOperation());
        }

        //if the worker was in failure when thre reduce started, and then has some additional work to do, we need to start the self_OperationResult
        if(self_OperationReduce == nullptr && operand == "reduce"){
            if(self_Operation != nullptr){
                cancelEvent(self_Operation);
            }
            self_OperationReduce = new cMessage("WORKER SELF REDUCE RESULT");
            scheduleAt(simTime()+randomNum(maxDelay, minDelay), self_OperationReduce);
        }

        vector<vector<tuple<int, int>>> partitions = mess->getVector();
        vector<int> indexes = mess->getIndeces();

        //we add the new additional work to the global variables
        //input_data.insert(input_data.end(), partitions.begin(), partitions.end());
        for(vector<tuple<int,int>> part: partitions){
            input_data.push_back(part);
        }
        //memoryIndeces.insert(memoryIndeces.end(), indexes.begin(), indexes.end());
        for(int i: indexes){
            memoryIndeces.push_back(i);
        }


    }

}

vector<tuple<int,int>> Worker::perfromReduceOperation(vector<tuple<int,int>> partition){
    //this is a variable to keep track of the keys in the partition vector that are already processed
    vector<int> valuesProcessed;

    vector<tuple<int,int>> reduceResult;

    EV<< "Partition computed: "<<endl;


    for(int i = 0; i < partition.size(); i++){
        int key = get<0>(partition.at(i));

        //EV << get<0>(partition.at(i))<< ", ";
        //EV << get<1>(partition.at(i))<< ", ";

        //we count how many elements in the valueProcessed having value = key. If it is null we skip, otherwise we search for all of the tuples
        //in partition with this key
        if(valuesProcessed.size() == 0 || !count(valuesProcessed.begin(), valuesProcessed.end(), key)){
            valuesProcessed.push_back(key);
            EV<<"values for "<<key<<" :";
            int sum = 0;
            for(int j = 0; j < partition.size(); j++){
                if(get<0>(partition.at(j)) == key){
                    EV<<get<1>(partition.at(j))<<", ";
                    sum += get<1>(partition.at(j));
                }
            }

            tuple<int,int> t;
            get<0>(t) = key;
            get<1>(t) = sum;
            reduceResult.push_back(t);

            EV<<"final result :";
            EV << get<0>(t)<< ", ";
            EV << get<1>(t)<< endl;
        }
    }
    return reduceResult;
}

vector<tuple<int,int>> Worker::performOperation(vector<tuple<int,int>> input_data, int memoryIndex){
    vector<tuple<int,int>> output_data;
    EV<< "Partition computed: "<<memoryIndex<<endl;
    switch (operand[0]){
        case 'm':
            //we apply the operation to each tuple and save in the output_data
            for(int j = 0; j < input_data.size(); j++){
                int f_v = choose_operand(operation, get<1>(input_data.at(j)), value); //we apply the funcion = operation to the 2nd eleement of the tuple, plus the value in the function(x)

                tuple<int,int> t;
                get<0>(t) = get<0>(input_data.at(j)); //key remains the same
                get<1>(t) = f_v; //value updated with f(v)
                output_data.push_back(t);

                EV << get<0>(t)<< ", ";
                EV << get<1>(t)<< endl;
            }
            break;
        case 'c':
            //we apply the operation to each tuple and save in the output_data
            for(int j = 0; j < input_data.size(); j++){
                int f_v = choose_operand(operation, get<1>(input_data.at(j)), value);

                tuple<int,int> t;
                //with changeKey, we start from <k, v> and we get <f(v), v>
                get<0>(t) = f_v; //key updated with f(v)
                get<1>(t) = get<1>(input_data.at(j)); //value remains the same
                output_data.push_back(t);

                EV << get<0>(t)<< ", ";
                EV << get<1>(t)<< endl;
            }
            break;
    }
    return output_data;
}

void Worker::sendFailureOperation(){
    PartitionMessage *tuplemess = new PartitionMessage(input_data, memoryIndeces, operationToDo, "FAILED PARTITIONS");

    //if the node has finished all its work, then we need to say to the coordinator that it is ok to drop this message
    if(input_data.size() == 0){
        tuplemess->changeFinish();
    }
    n_messages++; //for statistics
    send(tuplemess, "out");

    if(input_data.size() != 0){
        //we clear those two, in order to do not let the worker to send them when it will be active again
        input_data.clear();
        memoryIndeces.clear();
    }

    self_Restart = new cMessage("WORKER SELF RESTART");
    scheduleAt(simTime()+randomNum(maxRestart, minRestart), self_Restart);
}

void Worker::typeOfOperation(string operationToDo){
    //obtain the operand type
    string str = operationToDo; //map(ADD(2))
    size_t pos = str.find("(");
    operand = str.substr(0, pos);
    //EV << operand << endl;

    //obtain the function to apply
    string str1 = str.substr(pos+1, str.length()-2);
    pos = str1.find("(");
    operation = str1.substr(0, pos);
    //EV << operation << endl;

    //obtain the value
    string str2 = str1.substr(pos+1, str1.length()-1);
    pos = str2.find("(");
    value = convertString(str2, 0, pos);
    //EV << value << endl;
}

int Worker::convertString(string str, int start, int offset){
    int n;
    stringstream ss;
    string token = str.substr(start, offset);
    ss << token;
    ss >> n;
    return n;
}

int Worker::choose_operand(string s, int v, int v2){
   switch (s[0]){
        case 'A':
           return v+v2;
        case 'S':
            return v-v2;
        case 'M':
            return v*v2;
        case 'D':
            return v/v2;
   }
}

void Worker::cancelMessages(){
    finished = true;
}

int Worker::randomNum(int max, int min){
    return rand()%(max-min+1) + min;
}
